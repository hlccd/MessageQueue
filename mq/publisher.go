package mq

import (
	"fmt"
	"net"
)

type publisher struct {
	topic string
	conn  net.Conn
	ttl   uint8
}

func NewPublisher(topic string) (p *publisher) {
	return &publisher{
		topic: topic,
		conn:  nil,
		ttl:   15,
	}
}
func (p *publisher) Start(address string) (b bool) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
		return false
	}
	p.conn = conn
	h := newHead(publish, core, SYN, 1, 0)
	mes := h.createMessage(p.topic)
	fmt.Fprintf(p.conn, mes.string())
	mes, err = waitMessage(p.conn)
	if err != nil || mes == nil {
		return false
	} else if mes.h.operation == ACK {
		return true
	}
	return false
}
func (p *publisher) Publish(e interface{}) (success bool) {
	if p == nil || p.conn == nil {
		fmt.Println("未注册声明")
		return
	}
	h := newHead(publish, core, PSH, 15, 0)
	mes := h.createMessage(fmt.Sprintf("%v", e))
	fmt.Fprintf(p.conn, mes.string())
	mes, err := waitMessage(p.conn)
	if err != nil || mes == nil {
		return false
	} else if mes.h.operation == ACK {
		return true
	}
	return false
}
