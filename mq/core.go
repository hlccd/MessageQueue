package mq

import (
	"bufio"
	"fmt"
	"github.com/hlccd/goSTL/data_structure/deque"
	"net"
	"sync"
	"time"
)

type messageQueue struct {
	exit     chan bool                  //关闭消息队列
	cache    *deque.Deque               //信息缓存队列
	send     *deque.Deque               //消息发送队列
	capacity uint16                     //订阅时建立新管道的容量值
	ps       map[string][]chan *message //存储对应发布者/订阅者的管道集合
	pc       map[string]*deque.Deque    //存储生产者/消费者的中间消息管道
	sync.RWMutex
}
type cacheInfo struct {
	topic string
	mes   *message
}
type sendInfo struct {
	ch  chan *message
	mes *message
}

func NewCore() (mq *messageQueue) {
	mq = &messageQueue{
		exit:     make(chan bool),
		cache:    deque.New(),
		send:     deque.New(),
		capacity: 256,
		ps:       make(map[string][]chan *message),
		pc:       make(map[string]*deque.Deque),
	}
	go mq.cacheToSend()
	go mq.broadcast()
	return mq
}
func (mq *messageQueue) Start(point uint16) {
	fmt.Println("消息队列中心结点已启动,等到生产端和订阅端的链接")
	l, err := net.Listen("tcp", ":"+fmt.Sprintf("%d", point))
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
		}
		go mq.shunt(conn)
	}
}
func (mq *messageQueue) shunt(conn net.Conn) {
	topic := ""
	mes, err := waitMessage(conn)
	if err != nil || mes == nil {
		fmt.Fprintf(conn, "???\n")
		fmt.Println("非法or超时访问:", conn.RemoteAddr().String())
		conn.Close()
		return
	} else if mes.h.accepter == core && mes.h.operation == SYN {
		if mes.h.sender == core {
			fmt.Println("核心结点:", conn.RemoteAddr().String())
		} else if mes.h.sender == produce {
			go mq.listenPro(conn, topic)
		} else if mes.h.sender == consume {
			go mq.listenCon(conn, topic)
		} else if mes.h.sender == publish {
			go mq.listenPub(conn, topic)
		} else if mes.h.sender == subscribe {
			go mq.listenSub(conn, topic)
		}
	} else {
		fmt.Println("无效信息")
		conn.Close()
	}
}
func (mq *messageQueue) cacheToSend() {
	var e interface{}
	for {
		e = mq.cache.PopFront()
		if e != nil {
			switch e.(type) {
			case cacheInfo:
				info := e.(cacheInfo)
				mq.RLock()
				subscribers, ok := mq.ps[info.topic]
				mq.RUnlock()
				if ok {
					count := len(subscribers)
					concurrency := 1
					switch {
					case count > 524288:
						concurrency = 19
					case count > 131072:
						concurrency = 17
					case count > 8192:
						concurrency = 13
					case count > 2048:
						concurrency = 11
					case count > 128:
						concurrency = 7
					case count > 32:
						concurrency = 5
					case count > 8:
						concurrency = 3
					default:
						concurrency = 1
					}
					pub := func(start int) {
						for j := start; j < count; j += concurrency {
							sf := sendInfo{
								ch:  subscribers[j],
								mes: info.mes,
							}
							mq.send.PushBack(sf)
						}
					}
					for i := 0; i < concurrency; i++ {
						go pub(i)
					}
				}
			}
		}
	}
}
func (mq *messageQueue) broadcast() {
	var e interface{}
	for {
		e = mq.send.PopFront()
		if e != nil {
			switch e.(type) {
			case sendInfo:
				info := e.(sendInfo)
				info.mes.h.ttl--
				if info.mes.h.ttl > 0 {
					select {
					case info.ch <- info.mes:
						//推送成功
					case <-time.After(time.Millisecond * 5):
						//放入缓存重发序列中
						mq.send.PushBack(info)
					case <-mq.exit:
						continue
					}
				} else {
					fmt.Println("跳转次数过多")
				}
			}
		}
	}
}
func (mq *messageQueue) listenPro(conn net.Conn, topic string) {
	fmt.Println("生产结点:", conn.RemoteAddr().String())
	fmt.Println("topic:", topic)
	feedback(conn, core, produce, ACK)
	mq.Lock()
	d := mq.pc[topic]
	mq.Unlock()
	if d == nil {
		mq.Lock()
		mq.pc[topic] = deque.New()
		mq.Unlock()
	}
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("生产节点已断开", conn.RemoteAddr().String())
			conn.Close()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == PSH {
			h := newHead(core, consume, PSH, m.h.ttl, m.h.window)
			mes := h.createMessage(m.s)
			mq.Lock()
			mq.pc[topic].PushBack(mes)
			mq.Unlock()
			feedback(conn, core, produce, ACK)
		}
	}
}
func (mq *messageQueue) listenCon(conn net.Conn, topic string) {
	fmt.Println("消费结点:", conn.RemoteAddr().String())
	fmt.Println("topic:", topic)
	feedback(conn, core, consume, ACK)
	mq.Lock()
	d := mq.pc[topic]
	mq.Unlock()
	if d == nil {
		mq.Lock()
		mq.pc[topic] = deque.New()
		mq.Unlock()
	}
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("消费节点已断开", conn.RemoteAddr().String())
			conn.Close()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == GET {
			mq.Lock()
			e := mq.pc[topic].PopFront()
			mq.Unlock()
			if e != nil {
				switch e.(type) {
				case *message:
					mes := e.(*message)
					fmt.Fprintf(conn, mes.string())
					m, err := waitMessage(conn)
					if err != nil || m == nil {
						mq.Lock()
						mq.pc[topic].PushBack(e)
						mq.Unlock()
					} else if m.h.operation != ACK {
						mq.Lock()
						mq.pc[topic].PushBack(e)
						mq.Unlock()
					}
				}
			} else {
				feedback(conn, core, consume, NIL)
			}
		}
	}
}
func (mq *messageQueue) listenPub(conn net.Conn, topic string) {
	fmt.Println("发布结点:", conn.RemoteAddr().String())
	fmt.Println("topic:", topic)
	feedback(conn, core, publish, ACK)
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("发布节点已断开", conn.RemoteAddr().String())
			conn.Close()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == PSH {
			h := newHead(core, consume, PSH, m.h.ttl, m.h.window)
			mes := h.createMessage(m.s)
			info := cacheInfo{
				topic: topic,
				mes:   mes,
			}
			mq.Lock()
			mq.cache.PushBack(info)
			mq.Unlock()
			feedback(conn, core, publish, ACK)
		}
	}
}
func (mq *messageQueue) listenSub(conn net.Conn, topic string) {
	fmt.Println("订阅结点:", conn.RemoteAddr().String())
	fmt.Println("topic:", topic)
	feedback(conn, core, subscribe, ACK)
	sub := make(chan *message, mq.capacity)
	mq.Lock()
	mq.ps[topic] = append(mq.ps[topic], sub)
	mq.Unlock()
	unblocked := true
	go func() {
		for unblocked {
			for e := range sub {
				if e != nil && unblocked {
					h := newHead(core, subscribe, PSH, e.h.ttl, e.h.window)
					mes := h.createMessage(e.s)
					fmt.Fprintf(conn, mes.string())
				}
			}
		}
		fmt.Println("订阅节点已断开", conn.RemoteAddr().String())
	}()
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			unblocked = false
			conn.Close()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == FIN {
			unblocked = false
			mq.unsubscribe(sub, topic)
			feedback(conn, core, publish, ACK)
		}
	}
}
func (mq *messageQueue) unsubscribe(sub <-chan *message, topic string) {
	mq.RLock()
	subscribers, ok := mq.ps[topic]
	mq.RUnlock()
	if !ok {
		return
	}
	var newSubs []chan *message
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}
	mq.Lock()
	mq.ps[topic] = newSubs
	mq.Unlock()
}
