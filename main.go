package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"context"
)

// FIXME: To be removed
var (
	NATS_SERVER_URL = "localhost:4222"
	// messages are terminate by \r\n
	MSG_TERMINATE_BYTES = []byte{'\r', '\n'}
	OK_RESPONSE         = []byte("+OK\r\n")
)

type Client struct {
	conn               net.Conn
	serverInfo         []byte
	messagesReceived   chan []byte
	readMessageTimeout time.Duration

	connectionEstablised bool
}

func (c *Client) Connect(url string) error {
	// FIXME
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
	// NOTE: Is this really needed?
	startReadSync := make(chan struct{})
	go c.ingestMessages(context.TODO(), startReadSync)

	<-startReadSync
	close(startReadSync)

	readMsg := func(readTimeout time.Duration) ([]byte, error) {
		fmt.Printf("Messages channel size: %d\n", c.messagesReceived)
		select {
		case msg := <-c.messagesReceived:
			log.Println("reading message")
			return msg, nil
		case <-time.After(readTimeout):
			return nil, fmt.Errorf("read timeout exceeded (%s)", readTimeout)
		}
	}

	serverInfo, err := readMsg(c.readMessageTimeout)
	if err != nil {
		return fmt.Errorf("error reading server information: %w", err)
	}

	// Have something to clean up messages
	c.serverInfo = serverInfo[:len(serverInfo)-2]

	CONNECT := []byte("CONNECT {}\r\n")
	_, err = c.conn.Write(CONNECT)
	if err != nil {
		return fmt.Errorf("error sending CONNECT message to the server: %w", err)
	}

	okResponse, err := readMsg(c.readMessageTimeout)
	if err != nil {
		return fmt.Errorf("error reading OK response from the server: %w", err)
	}

	if !bytes.Equal(okResponse, OK_RESPONSE) {
		return fmt.Errorf("expected OK response from the server, got: %s", okResponse)
	}

	return nil
}

// NOTE: How do we use the context to cancel the ingestMessages loop?
func (c *Client) ingestMessages(ctx context.Context, syncChannel chan<- struct{}) ([]byte, error) {
	// Initialize the messagesReceived channel
	c.messagesReceived = make(chan []byte, 256)

	connReader := bufio.NewReader(c.conn)
	syncChannel <- struct{}{}
	for {
		response_msg_buf, err := connReader.ReadBytes(0x0a)
		if err != nil {
			if err != io.EOF {
				log.Printf("error reading from the server: %s", err)
			}
		}

		log.Printf("received message: %s", response_msg_buf)
		c.messagesReceived <- response_msg_buf
		log.Println("message inserted")
	}
}

func main() {
	// Establishing a connection
	natsClient := &Client{}
	err := natsClient.Connect(NATS_SERVER_URL)
	if err != nil {
		log.Fatalln(err)
	}
}
