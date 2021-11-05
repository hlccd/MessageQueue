package mq

import (
	"fmt"
	"net"
)

type consumer struct {
	topic string
	conn  net.Conn
}

func NewConsumer(topic string) (c *consumer) {
	return &consumer{
		topic: topic,
		conn:  nil,
	}
}
func (c *consumer) Start(address string) (b bool) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
		return false
	}
	c.conn = conn
	h := newHead(consume, core, SYN, 1, 0)
	mes := h.createMessage(c.topic)
	fmt.Fprintf(c.conn, mes.string())
	mes, err = waitMessage(c.conn)
	if err != nil || mes == nil {
		return false
	} else if mes.h.operation == ACK {
		return true
	}
	return false
}
func (c *consumer) Pull() (e interface{}) {
	if c == nil || c.conn == nil {
		return nil
	}
	h := newHead(consume, core, GET, 1, 1)
	mes := h.createMessage("")
	fmt.Fprintf(c.conn, mes.string())
	mes, err := waitMessage(c.conn)
	if err != nil || mes == nil {
		e = nil
	} else if mes.h.operation == PSH {
		e = mes.s
		feedback(c.conn, consume, core, ACK)
	} else if mes.h.operation == NIL {
		e = nil
	} else {
		e = nil
	}
	return e
}
