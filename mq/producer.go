package mq

import (
	"fmt"
	"net"
)

type producer struct {
	topic string
	conn  net.Conn
	ttl   uint8
}

func NewProducer(topic string) (p *producer) {
	return &producer{
		topic: topic,
		conn:  nil,
		ttl:   15,
	}
}
func (p *producer) Start(address string) (b bool) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
		return false
	}
	p.conn = conn
	h := newHead(produce, core, SYN, 1, 0)
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
func (p *producer) Produce(e interface{}) (success bool) {
	if p == nil || p.conn == nil {
		fmt.Println("未注册声明")
		return false
	}
	h := newHead(produce, core, PSH, 15, 0)
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
