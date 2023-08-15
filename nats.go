package nats

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"context"
)

var (
	// messages are terminated with \r\n
	MSG_TERMINATE_BYTES = []byte{'\r', '\n'}
)

type Operation string

const (
	INFO    Operation = "INFO"
	CONNECT           = "CONNECT"
	PUB               = "PUB"
	HPUB              = "HPUB"
	SUB               = "SUB"
	UNSUB             = "UNSUB"
	PING              = "PING"
	PONG              = "PONG"
	OK                = "+OK"
	ERR               = "-ERR"
)

type Message interface {
	MessagePayload() []byte
	OperationName() Operation
}

type ServerInfo struct {
	ServerId   string `json:"server_id"`
	ServerName string `json:"server_name"`
	Version    string `json:"version"`
	Protocol   int    `json:"proto"`
	GitCommit  string `json:"git_commit"`
	GoVersion  string `json:"go"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Headers    bool   `json:"headers"`
	MaxPayload int    `json:"max_payload"`
	ClientId   int    `json:"client_id"`
	ClientIp   string `json:"client_ip"`
	Cluster    string `json:"cluster"`
}

type InfoMessage struct {
	opName Operation
	ServerInfo
}

func (i *InfoMessage) MessagePayload() []byte {
	return nil
}

func (i *InfoMessage) OperationName() Operation {
	return i.opName
}

type OkMessage struct {
	opName Operation
}

func (o *OkMessage) MessagePayload() []byte {
	return nil
}

func (o *OkMessage) OperationName() Operation {
	return o.opName
}

type PingMessage struct {
	opName Operation
}

func (p *PingMessage) MessagePayload() []byte {
	return nil
}

func (p *PingMessage) OperationName() Operation {
	return p.opName
}

type SubMessage struct {
	opName     Operation
	subject    string
	queueGroup string
	sid        int
}

func (s *SubMessage) MessagePayload() []byte {
	return nil
}

func (s *SubMessage) OperationName() Operation {
	return s.opName
}

// FIXME: Find a better way to extract operation name and message payload from messages bytes
// and include additions checks
func parseMessage(messageBytes []byte) (Message, error) {
	var operationName []byte
	var messagePayload []byte

	for i, b := range messageBytes {
		if b == ' ' {
			operationName = messageBytes[:i]
			messagePayload = messageBytes[i:]
			break
		}
	}

	if len(operationName) == 0 {
		operationName = messageBytes[:len(messageBytes)-2]
	}

	switch Operation(operationName) {
	case INFO:
		msg := &InfoMessage{opName: INFO}
		err := json.Unmarshal(messagePayload, &msg)
		if err != nil {
			return nil, err
		}

		return msg, nil
	case OK:
		return &OkMessage{opName: OK}, nil
	case PING:
		return &PingMessage{opName: PING}, nil
	}

	return nil, nil
}

type Client struct {
	ServerInfo ServerInfo

	conn               net.Conn
	messagesChan       chan Message
	readMessageTimeout time.Duration

	connectionEstablished bool
}

func (c *Client) Connect(url string) error {
	// FIXME: Make ths configurable
	c.readMessageTimeout = 5 * time.Second

	conn, err := net.Dial("tcp", url)
	if err != nil {
		return fmt.Errorf("could not establish connection to the server: %w", err)
	}
	c.conn = conn

	err = c.initializeConnection()
	if err != nil {
		return fmt.Errorf("could not connect to the nats server: %w", err)
	}

	return nil
}

func (c *Client) initializeConnection() error {
	// FIXME: Is this really needed?
	startReadSync := make(chan struct{})
	go c.ingestMessages(context.TODO(), startReadSync)

	<-startReadSync
	close(startReadSync)

	readMsg := func(readTimeout time.Duration) (Message, error) {
		// fmt.Printf("Messages channel size: %d\n", len(c.messagesChan))
		select {
		case msg := <-c.messagesChan:
			return msg, nil
		case <-time.After(readTimeout):
			return nil, fmt.Errorf("read timeout exceeded (%s)", readTimeout)
		}
	}

	msg, err := readMsg(c.readMessageTimeout)
	if err != nil {
		return fmt.Errorf("error reading server information: %w", err)
	}

	infoMsg, ok := msg.(*InfoMessage)
	if !ok {
		return fmt.Errorf("expected INFO message from the server, got: %s", msg.OperationName())
	}

	c.ServerInfo = infoMsg.ServerInfo

	CONNECT := []byte("CONNECT {}\r\n")
	_, err = c.conn.Write(CONNECT)
	if err != nil {
		return fmt.Errorf("error sending CONNECT message to the server: %w", err)
	}

	okMessage, err := readMsg(c.readMessageTimeout)
	if err != nil {
		return fmt.Errorf("error reading OK response from the server: %w", err)
	}

	if okMessage.OperationName() != OK {
		return fmt.Errorf("expected OK response from the server, got: %s", okMessage.OperationName())
	}

	return nil
}

// NOTE: How do we use the context to cancel the ingestMessages loop?
func (c *Client) ingestMessages(ctx context.Context, syncChannel chan<- struct{}) ([]byte, error) {
	// Initialize the messagesReceived channel
	c.messagesChan = make(chan Message, 256)

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

		message, err := parseMessage(messageBytes)
		if err != nil {
			log.Printf("could not parse message: %s", err)
		}

		if _, ok := message.(*PingMessage); ok {
			// NOTE: For now, if the received message is a Ping, let's reply right
			// away and not include it in the messages channel
			if err := c.pong(); err != nil {
				log.Printf("could not write PONG message to the server: %s", err)
			}

			continue
		}

		log.Printf("Message: %v\n", message)
		c.messagesChan <- message
	}
}

func (c *Client) pong() error {
	log.Printf("Ping, Pong!\n")
	PONG := []byte("PONG\r\n")
	_, err := c.conn.Write(PONG)
	if err != nil {
		return err
	}

	return nil
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
	return line, nil
}
