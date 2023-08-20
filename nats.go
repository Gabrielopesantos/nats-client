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
	"strconv"
	"strings"
	"time"

	"context"
)

type CallbackFunction func(msg *ContentMessage)

var (
	// messages are terminated with \r\n
	MSG_TERMINATE_BYTES = []byte{'\r', '\n'}
)

type Client struct {
	ServerInfo ServerInfo

	conn net.Conn

	messages chan OperationMessage
	acks     chan struct{}

	subscriptions                 map[int]*Subscription // *?
	subscriptionMessages          map[int]chan *ContentMessage
	subscriptionCallbackFunctions map[int]CallbackFunction

	connectionEstablished bool

	readMessageTimeout time.Duration
	ackValidateTimeout time.Duration
}

func (c *Client) Connect(url string) error {
	// FIXME: Make these fields configurable
	c.readMessageTimeout = 5 * time.Second
	c.ackValidateTimeout = 5 * time.Second

	conn, err := net.Dial("tcp", url)
	if err != nil {
		return fmt.Errorf("could not establish connection to the server: %w", err)
	}
	c.conn = conn

	// Initialize the messagesReceived channel
	c.messages = make(chan OperationMessage, 256) // NOTE: Buffer size?
	c.acks = make(chan struct{})                  // FIXME: Not buffered?

	c.subscriptions = make(map[int]*Subscription)
	c.subscriptionMessages = make(map[int]chan *ContentMessage)
	c.subscriptionCallbackFunctions = make(map[int]CallbackFunction)

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

func (c *Client) initializeConnection() error {
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
func (c *Client) ingestMessages(ctx context.Context, syncChannel chan<- struct{}) ([]byte, error) {

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
			c.acks <- struct{}{}
		case *PingMessage:
			// NOTE: For now, if the received message is a Ping, let's reply right
			// away and not include it in the messages channel
			if err := c.pong(); err != nil {
				log.Printf("could not write PONG message to the server: %s", err)
			}
		case *ContentMessage:
			contentMsg := message.(*ContentMessage)
			sub := c.subscriptions[contentMsg.sid]
			contentMsg.Sub = sub

			c.subscriptionMessages[contentMsg.sid] <- contentMsg
		default:
			c.messages <- message
		}
	}
}

// FIXME: Remove logs
func (c *Client) ackReceived() bool {
	fmt.Println("Waiting for ACK")
	select {
	case <-c.acks:
		fmt.Println("ACK received")
		return true
	case <-time.After(c.ackValidateTimeout):
		fmt.Println("ACK timeout")
		return false
	}
}

func (c *Client) readMessage() (OperationMessage, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-time.After(c.readMessageTimeout):
		return nil, fmt.Errorf("read timeout exceeded (%s)", c.readMessageTimeout)
	}
}

func (c *Client) pong() error {
	PONG := []byte("PONG\r\n") // FIXME
	_, err := c.conn.Write(PONG)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Publish(subject string, payload []byte) error {
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

func (c *Client) ChanSubscribe(subject string, ch chan *ContentMessage) (*Subscription, error) {
	sub, err := c.subscribe(subject)
	if err != nil {
		return nil, err
	}

	c.subscriptions[sub.Sid] = sub
	c.subscriptionMessages[sub.Sid] = ch

	return sub, nil
}

func (c *Client) Subscribe(subject string, callbackFunc func(msg *ContentMessage)) error {
	sub, err := c.subscribe(subject)
	if err != nil {
		return err
	}

	c.subscriptions[sub.Sid] = sub
	// FIXME: Make channel buffer size configurable
	c.subscriptionMessages[sub.Sid] = make(chan *ContentMessage, 32)
	c.subscriptionCallbackFunctions[sub.Sid] = callbackFunc

	// FIXME
	go func(sid int) {
		for {
			if msg, ok := <-c.subscriptionMessages[sid]; ok {
				c.subscriptionCallbackFunctions[sid](msg)
			} else {
				break
			}
		}
	}(sub.Sid)

	return nil
}

func (c *Client) subscribe(subject string) (*Subscription, error) {
	sid := rand.Intn(100000) // NOTE: For testing purposes
	subMsg := SubscribeMessage{
		opName: SUB,
		Subscription: Subscription{
			Subject: subject,
			Sid:     sid,
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

func (c *Client) Unsubscribe(sub *Subscription) {
	close(c.subscriptionMessages[sub.Sid])

	// FIXME: These operations should be mutually exclusive
	delete(c.subscriptions, sub.Sid)
	delete(c.subscriptionMessages, sub.Sid)
	delete(c.subscriptionCallbackFunctions, sub.Sid)
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
