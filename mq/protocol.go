package mq

import (
	"errors"
)

const (
	headerLength = 5
)

const (
	master    = uint8(0)
	core      = uint8(1)
	produce   = uint8(2)
	consume   = uint8(3)
	publish   = uint8(4)
	subscribe = uint8(5)
)
const (
	SYN      = uint8(0)
	ACK      = uint8(1)
	PSH      = uint8(2)
	GET      = uint8(3)
	NIL      = uint8(4)
	FIN      = uint8(5)
	VISIT    = uint8(6)
	JOIN     = uint8(7)
	REDIRECT = uint8(8)
	KEEP     = uint8(9)
	AddCore  = uint8(11)
	DelCore  = uint8(12)
	GETPS    = uint8(13)
	GETPC    = uint8(14)
	VOTE     = uint8(15)
	VICTORY  = uint8(16)
	VICTOR   = uint8(17)
	UPDATE   = uint8(18)
)

type head struct {
	sender    uint8  //实际占4bit
	accepter  uint8  //实际占4bit
	operation uint8  //实际占8bit
	ttl       uint8  //实际占4bit
	window    uint16 //窗口大小,实际占12bit
	checksum  uint8
}
type message struct {
	h *head
	s string
}

func newHead(sender, accepter, opteration, ttl uint8, window uint16) (h *head) {
	h = &head{
		sender:    sender,
		accepter:  accepter,
		operation: opteration,
		ttl:       ttl,
		window:    window,
		checksum:  uint8(0),
	}
	bs := h.headToBytes()
	h.checksum = ^countSum(bs) + 1
	return h
}
func countSum(bs []byte) (sum uint8) {
	sum = 0
	tmp := uint16(0)
	for i := 0; i < headerLength-1; i++ {
		tmp += uint16(bs[i])
		sum = uint8(tmp>>8) + uint8(tmp)
		tmp = uint16(sum)
	}
	return sum
}
func (h *head) headToBytes() (bs []byte) {
	if h == nil {
		return bs
	}
	bs = make([]byte, headerLength)
	bs[0] = h.sender*16 + h.accepter%16
	bs[1] = h.operation
	bs[2] = h.ttl*16 + byte((h.window>>8)%16)
	bs[3] = byte(h.window)
	bs[headerLength-1] = h.checksum
	return bs
}
func bytesToHead(bs []byte) (h *head) {
	h = &head{
		sender:    bs[0] / 16,
		accepter:  bs[0] % 16,
		operation: bs[1],
		ttl:       bs[2] / 16,
		window:    uint16(bs[2]%16)<<8 + uint16(bs[3]),
		checksum:  bs[4],
	}
	return h
}
func (h *head) createMessage(s string) (mes *message) {
	mes = &message{
		h: h,
		s: s,
	}
	return mes
}
func (mes *message) string() (s string) {
	if mes == nil {
		return ""
	}
	s = string(mes.h.headToBytes()[:]) + mes.s + "\n"
	return s
}
func analysisMessage(mes []byte) (m *message, err error) {
	if len([]byte(mes)) < headerLength {
		return m, errors.New("error length")
	}
	m = &message{
		h: bytesToHead(mes),
		s: string(mes[headerLength:]),
	}
	if len(m.s) > 0 {
		if mes[len(mes)-1] == '\n' {
			m.s = m.s[:len(m.s)-1]
		}
	}
	bs := m.h.headToBytes()
	if m.h.checksum+countSum(bs) != 0 {
		return m, errors.New("checkSum error")
	}
	return m, nil
}
