package mq

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

func feedback(conn net.Conn, sender, accepter, opteration uint8) {
	if conn == nil {
		return
	}
	h := newHead(sender, accepter, opteration, 1, 1)
	mes := h.createMessage("")
	fmt.Fprintf(conn, mes.string())
}

func sendMessage(conn net.Conn, sender, accepter, opteration uint8, s string) {
	if conn == nil {
		return
	}
	h := newHead(sender, accepter, opteration, 1, 1)
	mes := h.createMessage(s)
	fmt.Fprintf(conn, mes.string())
}
func waitMessage(conn net.Conn, ts ...int) (mes *message, err error) {
	if conn == nil {
		return nil, errors.New("conn is nil")
	}
	ch := make(chan bool)
	mes = nil
	t := 1
	if len(ts) > 0 {
		t = ts[0]
	}
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
	case <-time.After(time.Duration(t) * time.Second):
		return nil, err
	case p := <-ch:
		if p {
			return mes, nil
		}
		return nil, err
	}
	return mes, nil
}
func addressTransition(senderAddr string, getAddr string) (address string) {
	address = ":"
	tmpSender := strings.SplitN(senderAddr, ":", 2)
	tmpGet := strings.SplitN(getAddr, ":", 2)
	if len(tmpSender) != 2 || len(tmpGet) != 2 {
		return address
	}
	if tmpGet[0] != "" {
		ss := strings.SplitN(tmpGet[0], ".", 4)
		if len(ss) != 4 {
			return address
		} else {
			if ss[0] == "127" && (ss[1] != "0" || ss[2] != "0" || ss[3] != "0") {
				tmpGet[0] = tmpSender[0]
			}
		}
	} else {
		tmpGet[0] = tmpSender[0]
	}
	address = tmpGet[0] + ":" + tmpGet[1]
	return address
}
