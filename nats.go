package nats

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"context"
)

type CallbackFunction func(msg *ContentMessage)

// Default connection options
const (
	DEFAULT_NATS_SERVER_URL      = "nats://localhost:4222"
	DEFAULT_READ_TIMEOUT         = 5 * time.Second
	DEFAULT_VALIDATE_ACK_TIMEOUT = 5 * time.Second
	DEFAULT_SUB_CHAN_LEN         = 256
)

var (
	// messages are terminated with \r\n
	MSG_TERMINATE_BYTES = []byte{'\r', '\n'}
)

type Connection struct {
	ServerInfo            ServerInfo
	conn                  net.Conn
	opts                  *Options
	msgsCh                chan OperationMessage
	acksCh                chan struct{}
	subs                  map[int]*Subscription
	subMu                 sync.RWMutex
	connectionEstablished bool

	sync.Mutex
}

type Options struct {
	ServerUrl          string
	ReadMessageTimeout time.Duration
	AckValidateTimeout time.Duration
	SubChannelLen      uint
}

func DefaultOptions() *Options {
	return &Options{
		ServerUrl:          DEFAULT_NATS_SERVER_URL,
		ReadMessageTimeout: DEFAULT_READ_TIMEOUT,
		AckValidateTimeout: DEFAULT_VALIDATE_ACK_TIMEOUT,
		SubChannelLen:      DEFAULT_SUB_CHAN_LEN,
	}
}

func Connect(options *Options) (*Connection, error) {
	if options == nil {
		options = DefaultOptions()
	}

	nc := &Connection{opts: options}
	err := nc.connect()
	if err != nil {
		return nil, err
	}

	return nc, err
}

func (c *Connection) connect() error {
	// NOTE: Skipping schema check
	url, err := url.Parse(c.opts.ServerUrl)
	if err != nil {
		return fmt.Errorf("could not parse server url: %w", err)
	}

	conn, err := net.Dial("tcp", url.Host)
	if err != nil {
		return fmt.Errorf("could not establish connection to the server: %w", err)
	}
	c.conn = conn

	// Initialize the messagesReceived channel
	c.msgsCh = make(chan OperationMessage, 256) // NOTE: Buffer size?
	c.acksCh = make(chan struct{})              // FIXME: Not buffered?

	c.subs = make(map[int]*Subscription)

	// FIXME: Is this really needed?
	startReadSync := make(chan struct{})
	go c.ingestMessages(context.TODO(), startReadSync)

	<-startReadSync
	close(startReadSync)

	err = c.initializeConnection()
	if err != nil {
		return fmt.Errorf("could not connect to the nats server: %w", err)
	}

	return nil
}

func (c *Connection) initializeConnection() error {
	msg, err := c.readMessage()
	if err != nil {
		return fmt.Errorf("error reading server information: %w", err)
	}

	infoMsg, ok := msg.(*InfoMessage)
	if !ok {
		return fmt.Errorf("expected INFO message from the server, got: %s", msg.OperationName())
	}

	c.ServerInfo = infoMsg.ServerInfo

	CONNECT := []byte("CONNECT {}\r\n") // FIXME
	_, err = c.conn.Write(CONNECT)
	if err != nil {
		return fmt.Errorf("error sending CONNECT message to the server: %w", err)
	}

	if !c.ackReceived() {
		return fmt.Errorf("did not receive ACK from the server")
	}

	return nil
}

// NOTE: How do we use the context to cancel the ingestMessages loop?
func (c *Connection) ingestMessages(ctx context.Context, syncChannel chan<- struct{}) ([]byte, error) {

	connReader := bufio.NewReader(c.conn)
	syncChannel <- struct{}{}
	for {
		messageBytes, err := readMessagePayload(connReader, MSG_TERMINATE_BYTES)
		if err != nil {
			// FIXME: Understand if this check makes sense
			if err != io.EOF {
				log.Printf("error reading from the server: %s", err)
			}
		}

		// log.Printf("Message bytes: %s", messageBytes)
		message, err := parseMessage(messageBytes)
		if err != nil {
			log.Printf("could not parse message: %s", err)
		}
		log.Printf("Message: %v\n", message)

		switch message.(type) {
		case *OkMessage:
			c.acksCh <- struct{}{}
		case *PingMessage:
			// NOTE: For now, if the received message is a Ping, let's reply right
			// away and not include it in the messages channel
			if err := c.pong(); err != nil {
				log.Printf("could not write PONG message to the server: %s", err)
			}
		case *ContentMessage:
			contentMsg := message.(*ContentMessage)
			// Check if subscription exists
			sub, ok := c.subs[contentMsg.sid]
			if !ok {
				continue
			}

			contentMsg.Sub = sub

			// FIXME: Check if messagesChan hasn't been closed?
			// full?
			sub.msgsCh <- contentMsg
		default:
			c.msgsCh <- message
		}
	}
}

// FIXME: Remove logs
func (c *Connection) ackReceived() bool {
	fmt.Println("Waiting for ACK")
	select {
	case <-c.acksCh:
		fmt.Println("ACK received")
		return true
	case <-time.After(c.opts.AckValidateTimeout):
		fmt.Println("ACK timeout")
		return false
	}
}

func (c *Connection) readMessage() (OperationMessage, error) {
	select {
	case msg := <-c.msgsCh:
		return msg, nil
	case <-time.After(c.opts.ReadMessageTimeout):
		return nil, fmt.Errorf("read timeout exceeded (%s)", c.opts.ReadMessageTimeout)
	}
}

func (c *Connection) pong() error {
	PONG := []byte("PONG\r\n") // FIXME
	_, err := c.conn.Write(PONG)
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) Publish(subject string, payload []byte) error {
	pubMsg := PublishMessage{
		opName:  PUB,
		subject: subject,
		nBytes:  len(payload),
		payload: payload,
	}

	_, err := c.conn.Write(pubMsg.FormattedMessage())
	if err != nil {
		return fmt.Errorf("could not write PUB message to the server: %w", err)
	}

	// NOTE: De we want validate the ACK on publish?
	if !c.ackReceived() {
		return fmt.Errorf("did not receive ACK from the server")
	}

	return nil
}

func (c *Connection) ChanSubscribe(subject string, ch chan *ContentMessage) (*Subscription, error) {
	sub, err := c.subscribe(subject)
	if err != nil {
		return nil, err
	}

	// Override
	sub.msgsCh = ch
	c.registerSubscription(sub)

	return sub, nil
}

func (c *Connection) Subscribe(subject string, callbackFunc func(msg *ContentMessage)) error {
	sub, err := c.subscribe(subject)
	if err != nil {
		return err
	}

	sub.cbFn = callbackFunc
	c.registerSubscription(sub)

	// FIXME
	go func(sub *Subscription) {
		for {
			if msg, ok := <-sub.msgsCh; ok {
				sub.cbFn(msg)
			} else {
				break
			}
		}
	}(sub)

	return nil
}

func (c *Connection) SubscribeSync(subject string) (*Subscription, error) {
	sub, err := c.subscribe(subject)
	if err != nil {
		return nil, err
	}

	c.registerSubscription(sub)

	return sub, nil
}

func (c *Connection) subscribe(subject string) (*Subscription, error) {
	sid := rand.Intn(100000) // NOTE: For testing purposes
	subMsg := SubscribeMessage{
		opName: SUB,
		Subscription: Subscription{
			Subject: subject,
			Sid:     sid,

			// FIXME: Make channel buffer size configurable
			msgsCh: make(chan *ContentMessage, c.opts.SubChannelLen),
		},
	}

	_, err := c.conn.Write(subMsg.OperationMessage())
	if err != nil {
		return nil, fmt.Errorf("could not write SUB message to the server: %w", err)
	}

	if !c.ackReceived() {
		return nil, fmt.Errorf("did not receive ACK from the server")
	}

	return &subMsg.Subscription, nil
}

func (c *Connection) Unsubscribe(sub *Subscription) {
	c.removeSubscription(sub)
}

func (c *Connection) registerSubscription(sub *Subscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.subs[sub.Sid] = sub
}

func (c *Connection) removeSubscription(sub *Subscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	close(sub.msgsCh)
	delete(c.subs, sub.Sid)
}

// FIXME: Reads data byte-by-byte and checks for the delimiter after each byte read,
// which could be inefficient if processing large amounts of data or if the delimiter is long.
func readMessagePayload(r *bufio.Reader, delim []byte) ([]byte, error) {
	var line []byte
	for {
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		line = append(line, c)
		if bytes.HasSuffix(line, delim) {
			break
		}
	}

	// FIXME: Has messages (MSG) payload is separated from the opeartion data with \r\n
	// this allows reading the message data to the same slice
	if bytes.HasPrefix(line, []byte("MSG")) {
		for {
			c, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			line = append(line, c)
			if bytes.HasSuffix(line, delim) {
				break
			}
		}
	}

	return line, nil
}

// FIXME: Find a better way to extract operation name and message payload from messages bytes
// and include additions checks
func parseMessage(messageBytes []byte) (OperationMessageReceive, error) {
	var operationName []byte
	var messageData []byte

	for i, b := range messageBytes {
		if b == ' ' {
			operationName = messageBytes[:i]
			messageData = messageBytes[i:]
			break
		}
	}

	if len(operationName) == 0 {
		operationName = messageBytes[:len(messageBytes)-2]
	}

	switch Operation(operationName) {
	case INFO:
		msg := &InfoMessage{opName: INFO}
		err := json.Unmarshal(messageData, &msg)
		if err != nil {
			return nil, err
		}

		return msg, nil
	case OK:
		return &OkMessage{opName: OK}, nil
	case PING:
		return &PingMessage{opName: PING}, nil
	case MSG:
		// FIXME
		messageDataStr := strings.Trim(string(messageData), "\r\n ")
		messageParts := strings.Split(messageDataStr, "\r\n")
		messageInfo := strings.Split(messageParts[0], " ")

		subject := messageInfo[0]
		sid, _ := strconv.Atoi(messageInfo[1])
		nBytes, _ := strconv.Atoi(messageInfo[2])
		data := []byte(messageParts[1])

		// FIXME: Assuming `reply-to` will not be in the message for now
		return &ContentMessage{
			opName:  MSG,
			Subject: subject,
			sid:     sid,
			nBytes:  nBytes,
			Data:    data,
		}, nil
	}

	return nil, nil
}
