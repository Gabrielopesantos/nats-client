package nats

import (
	"fmt"
	"time"
)

type Operation string

const (
	INFO    Operation = "INFO"
	CONNECT           = "CONNECT"
	PUB               = "PUB"
	HPUB              = "HPUB"
	SUB               = "SUB"
	UNSUB             = "UNSUB"
	MSG               = "MSG"
	PING              = "PING"
	PONG              = "PONG"
	OK                = "+OK"
	ERR               = "-ERR"
)

type OperationMessage interface {
	OperationName() Operation
}

type OperationMessageReceive interface {
	MessagePayload() []byte
	OperationMessage
}

type OperationMessageSend interface {
	FormattedMessage() []byte
	OperationMessage
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

func (i *InfoMessage) OperationName() Operation {
	return i.opName
}

func (i *InfoMessage) MessagePayload() []byte {
	return nil
}

type ConnectMessage struct {
	opName Operation
}

func (c *ConnectMessage) OperationName() Operation {
	return c.opName
}

func (c *ConnectMessage) FormattedMessage() []byte {
	return []byte(fmt.Sprintf("%s {}\r\n", c.opName))
}

type OkMessage struct {
	opName Operation
}

func (o *OkMessage) OperationName() Operation {
	return o.opName
}
func (o *OkMessage) MessagePayload() []byte {
	return nil
}

type PingMessage struct {
	opName Operation
}

func (p *PingMessage) OperationName() Operation {
	return p.opName
}
func (p *PingMessage) MessagePayload() []byte {
	return nil
}

type PongMessage struct {
	opName Operation
}

func (p *PongMessage) OperationName() Operation {
	return p.opName
}

func (p *PongMessage) FormattedMessage() []byte {
	return []byte(fmt.Sprintf("%s\r\n", p.opName))
}

type PublishMessage struct {
	opName  Operation
	subject string
	replyTo string
	nBytes  int
	payload []byte
}

func (p *PublishMessage) OperationName() Operation {
	return p.opName
}

func (p *PublishMessage) FormattedMessage() []byte {
	if p.replyTo == "" {
		return []byte(fmt.Sprintf("%s %s %d\r\n%s\r\n", p.opName, p.subject, p.nBytes, p.payload))
	}

	return []byte(fmt.Sprintf("%s %s %s %d\r\n%s\r\n", p.opName, p.subject, p.replyTo, p.nBytes, p.payload))
}

type SubscribeMessage struct {
	opName Operation
	Subscription
}

func (s *SubscribeMessage) OperationName() Operation {
	return s.opName
}

func (s *SubscribeMessage) MessagePayload() []byte {
	return nil
}

func (s *SubscribeMessage) OperationMessage() []byte {
	return []byte(fmt.Sprintf("%s %s %s %d\r\n", s.opName, s.Subject, s.QueueGroup, s.Sid))
}

type Subscription struct {
	Sid        int
	Subject    string
	QueueGroup string

	messagesChan chan *ContentMessage
	callbackFn   CallbackFunction
}

func (s *Subscription) NextMessage(readTimeout time.Duration) (*ContentMessage, error) {
	select {
	case msg := <-s.messagesChan:
		return msg, nil
	case <-time.After(readTimeout):
		return nil, fmt.Errorf("read timeout exceeded (%s)", readTimeout)
	}
}

// FIXME: Overlaps with PublishMessage ???
type ContentMessage struct {
	opName  Operation
	Subject string
	sid     int
	Reply   string
	Data    []byte
	nBytes  int
	Sub     *Subscription
}

func (c *ContentMessage) OperationName() Operation {
	return c.opName
}

func (c *ContentMessage) MessagePayload() []byte {
	return c.Data
}
