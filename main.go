package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

// FIXME: To be removed
var (
	NATS_SERVER_URL = "localhost:4222"
	// messages are terminate by \r\n
	MSG_TERMINATE_BYTES = []byte{'\r', '\n'}
	OK_RESPONSE         = []byte("+OK\r\n")
)

type Client struct {
	conn       net.Conn
	serverInfo []byte
}

func (c *Client) Connect(url string) error {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		return fmt.Errorf("could not establish connection the server: %w", err)
	}
	c.conn = conn

	err = c.initializeConnection()
	if err != nil {
		return fmt.Errorf("error connecting with the nats server: %w", err)
	}

	return nil
}

func (c *Client) initializeConnection() error {
	connReader := bufio.NewReader(c.conn)

	// Read server info
	response_msg_buf, err := connReader.ReadBytes(0x0a)
	if err != nil {
		if err != io.EOF {
			log.Printf("error reading from the server: %s", err)
		}
	}

	// Log results
	fmt.Printf("response: %s", response_msg_buf)
	fmt.Println("total response size:", len(response_msg_buf))

	// Send empty CONNECT message
	c.serverInfo = response_msg_buf

	_, err = c.conn.Write([]byte("CONNECT {}\r\n"))
	if err != nil {
		return fmt.Errorf("error sending CONNECT operation message: %w", err)
	}

	response_msg_buf, err = connReader.ReadBytes(0x0a)
	if err != nil {
		if err != io.EOF {
			log.Printf("error reading from the server: %s", err)
		}
	}

	// Log results
	fmt.Printf("response: %s", response_msg_buf)
	fmt.Println("total response size:", len(response_msg_buf))
	if string(response_msg_buf) != string(OK_RESPONSE) {
		return fmt.Errorf("server did not respond with an OK message: %w", err)
	}

	return nil
}

func main() {
	// Establishing a connection
	natsClient := &Client{}
	err := natsClient.Connect(NATS_SERVER_URL)
	if err != nil {
		log.Fatalln(err)
	}
}
