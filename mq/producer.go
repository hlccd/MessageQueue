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
	sendMessage(conn, produce, core, GETPC, p.topic)
	mes, err := waitMessage(conn, 10)
	if err != nil || mes == nil {
		return false
	}
	if mes.h.operation == ACK {
		addr := addressTransition(conn.RemoteAddr().String(), mes.s)
		if addr == ":" {
			return false
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println(err)
			return false
		}
		p.conn = conn
		sendMessage(conn, produce, core, SYN, p.topic)
		mes, err := waitMessage(p.conn, 1)
		if err != nil || mes == nil {
			return false
		} else if mes.h.operation == ACK {
			return true
		}
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
