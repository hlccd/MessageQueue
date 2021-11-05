package mq

import (
	"bufio"
	"fmt"
	"net"
)

type subscriber struct {
	topic     string
	cache     chan interface{}
	conn      net.Conn
	unblocked bool
}

func NewSubscriber(topic string) (s *subscriber) {
	s = &subscriber{
		topic:     topic,
		cache:     make(chan interface{}, 1024),
		conn:      nil,
		unblocked: false,
	}
	return s
}
func (s *subscriber) Start(address string) (b bool) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
		return false
	}
	s.conn = conn
	h := newHead(subscribe, core, SYN, 1, 1)
	mes := h.createMessage(s.topic)
	fmt.Fprintf(s.conn, mes.string())
	mes, err = waitMessage(s.conn)
	if err != nil || mes == nil {
		return false
	} else if mes.h.operation == ACK {
		s.unblocked = true
		go s.poll()
		return true
	}
	return false
}
func (s *subscriber) poll() {
	for {
		if s == nil || !s.unblocked || s.conn == nil {
			return
		}
		data, err := bufio.NewReader(s.conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			s.conn.Close()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			fmt.Println(err)
			continue
		}
		s.cache <- m.s
	}
}
func (s *subscriber) Get() (e interface{}) {
	if s == nil {
		return nil
	}
	for val := range s.cache {
		if val != nil {
			return val
		}
	}
	return nil
}
func (s *subscriber) Unsubscribe() {
	if s == nil || s.conn == nil {
		return
	}
	h := newHead(subscribe, core, FIN, 1, 1)
	mes := h.createMessage("")
	fmt.Fprintf(s.conn, mes.string())
	s.conn.Close()
}
