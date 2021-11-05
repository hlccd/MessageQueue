package mq

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func feedback(conn net.Conn, sender, accepter, opteration uint8) {
	h := newHead(sender, accepter, opteration, 1, 1)
	mes := h.createMessage("")
	fmt.Fprintf(conn, mes.string())
}
func waitMessage(conn net.Conn) (mes *message, err error) {
	ch := make(chan bool)
	mes = nil
	go func() {
		data := ""
		data, err = bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			conn.Close()
			ch <- false
			return
		}
		mes, err = analysisMessage([]byte(data))
		if err != nil {
			fmt.Println(err)
			ch <- false
			return
		}
		ch <- true
	}()
	select {
	case <-time.After(time.Second):
		return nil, err
	case p := <-ch:
		if p {
			return mes, nil
		}
		return nil, err
	}
	return mes, nil
}
