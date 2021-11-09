package mq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/hlccd/goSTL/data_structure/deque"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type messageQueue struct {
	id       int64                      //该结点id号,纳秒时间戳
	master   net.Conn                   //核心集群中的master结点连接
	list     []cores                    //该结点集群的其他结点地址
	exit     chan bool                  //关闭消息队列
	cache    *deque.Deque               //信息缓存队列
	send     *deque.Deque               //消息发送队列
	capacity uint16                     //订阅时建立新管道的容量值
	ps       map[string][]chan *message //存储对应发布者/订阅者的管道集合
	pc       map[string]*deque.Deque    //存储生产者/消费者的中间消息管道
	lastKeep int64
	vote     int64
	point    string
	inVote   bool
	sync.RWMutex
}
type cores struct {
	Id    int64  `json:"id"`
	Addr  string `json:"addr"`
	Power uint8  `json:"power"`
	conn  net.Conn
	vote  int64
}
type cacheInfo struct {
	topic string
	mes   *message
}
type sendInfo struct {
	ch  chan *message
	mes *message
}

func NewCore(addrs ...string) (mq *messageQueue) {
	mq = &messageQueue{
		id:       time.Now().UnixNano(),
		master:   nil,
		list:     make([]cores, 0, 0),
		exit:     make(chan bool),
		cache:    deque.New(),
		send:     deque.New(),
		capacity: 256,
		ps:       make(map[string][]chan *message),
		pc:       make(map[string]*deque.Deque),
		lastKeep: time.Now().UnixNano(),
		vote:     0,
		point:    ":",
		inVote:   false,
	}
	if len(addrs) != 0 {
		for i := range addrs {
			conn, err := net.Dial("tcp", addrs[i])
			if err == nil {
				//接入集群
				sendMessage(conn, core, core, VISIT, "")
				mes, err := waitMessage(conn)
				if err != nil || mes == nil {
					continue
				} else if mes.h.operation == ACK {
					var list []cores
					err := json.Unmarshal([]byte(mes.s), &list)
					if err != nil {
						continue
					} else {
						mq.list = list
					}
					break
				}
			}
		}
	}
	go mq.cacheToSend()
	go mq.broadcast()
	return mq
}
func (mq *messageQueue) Start(point uint16) {
	if mq == nil {
		return
	}
	//链接中心结点
	addr := ""
	for l := range mq.list {
		if mq.list[l].Power == master {
			addr = mq.list[l].Addr
			break
		}
	}
	mq.point = ":" + fmt.Sprintf("%d", point)
	//无master结点
	if addr == "" {
		//将自己设为master结点
		mq.list = append(mq.list, cores{
			Id:    mq.id,
			Addr:  ":" + fmt.Sprintf("%d", point),
			Power: master,
		})
		go mq.masterKeep()
	} else {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			c := cores{
				Id:    mq.id,
				Addr:  mq.point,
				Power: core,
			}
			s, _ := json.Marshal(c)
			sendMessage(conn, core, core, JOIN, string(s))
			mes, err := waitMessage(conn)
			if err != nil || mes == nil {
				//链接master结点失败,master结点可能宕机
				return
			} else if mes.h.operation == ACK {
				//链接master节点成功
				var list []cores
				json.Unmarshal([]byte(mes.s), &list)
				list = append(list, c)
				mq.Lock()
				mq.master = conn
				mq.list = list
				mq.Unlock()
				go mq.listenMaster()
			} else {
				return
			}
		}
	}
	fmt.Println("消息队列中心结点已启动,等到生产端和订阅端的链接")
	l, err := net.Listen("tcp", mq.point)
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
	mes, err := waitMessage(conn)
	if err != nil || mes == nil {
		fmt.Fprintf(conn, "???\n")
		fmt.Println("非法or超时访问:", conn.RemoteAddr().String())
		conn.Close()
		return
	} else if mes.h.sender != master && mes.h.accepter == core {
		if mes.h.operation == GETPS || mes.h.operation == GETPC {
			//请求建立连接,从本核心获取topic信息,若无则从master处获取
			go mq.redirect(conn, mes.h.sender, mes.h.operation, mes.s)
		} else if mes.h.operation == SYN {
			topic := mes.s
			if mes.h.sender == produce {
				go mq.listenPro(conn, topic)
			} else if mes.h.sender == consume {
				go mq.listenCon(conn, topic)
			} else if mes.h.sender == publish {
				go mq.listenPub(conn, topic)
			} else if mes.h.sender == subscribe {
				go mq.listenSub(conn, topic)
			}
		}
		if mes.h.sender == core {
			if mes.h.operation == VISIT {
				if ss, err := json.Marshal(mq.list); err == nil {
					sendMessage(conn, core, core, ACK, string(ss))
					conn.Close()
				}
			} else if mes.h.operation == JOIN {
				if mq.master == nil {
					//应添加id和端口号
					var c cores
					json.Unmarshal([]byte(mes.s), &c)
					if err != nil {
						feedback(conn, core, core, NIL)
						return
					} else {
						go mq.addCore(conn, c.Id, c.Addr)
						go mq.listenCore(conn, c.Id)
					}
				} else {
					feedback(conn, core, core, NIL)
				}
			} else if mes.h.operation == VOTE {
				fmt.Println("address:", conn.RemoteAddr().String(), "找我拉票")
				if mq.vote == 0 || time.Now().UnixNano()-mq.lastKeep > int64(90*time.Second) {
					mq.lastKeep = time.Now().UnixNano()
					mq.vote, err = strconv.ParseInt(mes.s, 10, 64)
					if err != nil {
						feedback(conn, core, core, NIL)
					} else {
						//支持拉票者
						sendMessage(conn, core, core, ACK, strconv.FormatInt(mq.vote, 10))
					}
				} else {
					//已支持其他人
					sendMessage(conn, core, core, ACK, strconv.FormatInt(mq.vote, 10))
				}
				fmt.Println("我当前支持:", mq.vote)
				go mq.listenVoter(conn)
			}
		}
	} else if mes.h.sender == master {
		if mes.h.operation == GETPS || mes.h.operation == GETPC {
			mq.RLock()
			ans := "0"
			ok := false
			if mes.h.operation == GETPS {
				_, ok = mq.ps[mes.s]
			} else {
				_, ok = mq.pc[mes.s]
			}
			if !ok {
				ans = strconv.Itoa(len(mq.ps) + len(mq.pc))
			}
			sendMessage(conn, core, master, ACK, ans)
			mq.RUnlock()
		}
	} else {
		fmt.Println("无效信息")
		conn.Close()
	}
}
func (mq *messageQueue) redirect(conn net.Conn, sender uint8, operation uint8, topic string) {
	addr := ":"
	mq.RLock()
	if operation == GETPC {
		if _, ok := mq.pc[topic]; ok {
			addr = mq.point
		}
	} else {
		if _, ok := mq.ps[topic]; ok {
			addr = mq.point
		}
	}
	mq.RUnlock()
	if addr != ":" {
		sendMessage(conn, core, sender, ACK, addr)
	} else if mq.master == nil {
		//我是master结点
		addr = mq.masterRedirect(conn, sender, operation, topic)
		if addr != ":" {
			//返回给发送者,可能是中心结点也可能是pc/ps结点
			sendMessage(conn, core, sender, ACK, addr)
		}
	} else {
		//我不是master结点
		masterAddr := ":"
		mq.RLock()
		//寻找master结点地址并获取
		for i := range mq.list {
			if mq.list[i].Power == master {
				masterAddr = mq.list[i].Addr
				break
			}
		}
		mq.RUnlock()
		if masterAddr == ":" {
			sendMessage(conn, core, sender, NIL, mq.point)
			return
		}
		masterConn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			sendMessage(conn, core, sender, NIL, mq.point)
			return
		}
		sendMessage(masterConn, core, core, operation, topic)
		mes, err := waitMessage(masterConn, 3)
		if err != nil || mes == nil {
			sendMessage(conn, core, sender, NIL, mq.point)
			return
		}
		if mes.h.operation == ACK {
			//接收成功
			addr = addressTransition(masterConn.RemoteAddr().String(), mes.s)
			sendMessage(conn, core, sender, ACK, addr)
		}
	}
}
func (mq *messageQueue) masterRedirect(conn net.Conn, sender uint8, operation uint8, topic string) (addr string) {
	addr = ":"
	mq.RLock()
	if operation == GETPC {
		if _, ok := mq.pc[topic]; ok {
			addr = mq.point
		}
	} else {
		if _, ok := mq.ps[topic]; ok {
			addr = mq.point
		}
	}
	mq.RUnlock()
	if addr != ":" {
		return addr
	} else {
		mq.RLock()
		list := mq.list
		mq.RUnlock()
		wg := sync.WaitGroup{}
		lock := sync.Mutex{}
		num := len(mq.pc) + len(mq.ps)
		addr = mq.point
		for i := range list {
			if list[i].Id != mq.id && list[i].conn != nil {
				wg.Add(1)
				go func(c cores) {
					conn, err := net.Dial("tcp", list[i].Addr)
					sendMessage(conn, master, core, operation, topic)
					mes, err := waitMessage(conn)
					if err != nil || mes == nil {

					} else if mes.h.operation == ACK {
						ans, err := strconv.Atoi(mes.s)
						if err == nil {
							lock.Lock()
							if ans < num {
								addr = c.Addr
								num = ans
							}
							lock.Unlock()
						}
					}
					wg.Done()
				}(list[i])
			}
		}
		wg.Wait()
	}
	return addr
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
func (mq *messageQueue) masterKeep() {
	for i := 5; i >= 0 && mq.master == nil; i++ {
		time.Sleep(30 * time.Second)
		if mq.master == nil {
			for j := range mq.list {
				sendMessage(mq.list[j].conn, master, core, KEEP, "")
			}
			if i >= 5 {
				ss, err := json.Marshal(mq.list)
				if err == nil {
					for j := range mq.list {
						sendMessage(mq.list[j].conn, master, core, UPDATE, string(ss))
					}
				}
				i = 0
			}
		}
	}
}
func (mq *messageQueue) voteMaster() {
	mq.lastKeep = 0
	mq.vote = 0
	fmt.Println("发起选举")
	rand.Seed(time.Now().UnixNano() * mq.id)
	i := rand.Intn(10000)
	time.Sleep(time.Duration(i+1000) * time.Millisecond)
	mq.Lock()
	mq.inVote = true
	if mq.vote == 0 {
		mq.master = nil
		mq.vote = mq.id
		mq.lastKeep = time.Now().UnixNano()
		//随机等待后master结点选举开始
		//选举过程采用先到先得,率先获得最高票者为master,票数相同则id更大的为master
		wg := sync.WaitGroup{}
		m := make(map[int64]int64)
		m[mq.id] = 1
		fmt.Println("我参选")
		voteList := make([]cores, 0, len(mq.list)-1)
		for i := range mq.list {
			if mq.list[i].Power == core && mq.list[i].Id != mq.id {
				wg.Add(1)
				go func(c cores) {
					conn, err := net.Dial("tcp", c.Addr)
					if err == nil {
						sendMessage(conn, core, core, VOTE, strconv.FormatInt(mq.id, 10))
						mes, err := waitMessage(conn)
						if err != nil || mes == nil {
							//链接失败
							c.vote = 0
						} else if mes.h.operation == ACK {
							//链接成功,获取对方支持结果
							c.conn = conn
							c.vote, err = strconv.ParseInt(mes.s, 10, 64)
							if err == nil {
								voteList = append(voteList, c)
							} else {
								c.vote = 0
							}
						}
					} else {
						c.vote = 0
					}
					m[c.vote]++
					wg.Done()
				}(mq.list[i])
			}
		}
		wg.Wait()
		//投票计算
		victor := mq.id
		poll := m[mq.id]
		for k, v := range m {
			if k != 0 {
				if v > poll {
					poll = v
					victor = k
				} else if v == poll && k > victor {
					poll = v
					victor = k
				}
			}
		}
		//投票完成
		if victor == mq.id {
			mq.master = nil
			time.Sleep(15 * time.Second)
			//通知所有链接成功的点我已胜选
			c := cores{
				Id:    mq.id,
				Addr:  mq.point,
				Power: master,
			}
			list := make([]cores, 0, len(mq.list)-1)
			list = append(list, c)
			for i := range voteList {
				if voteList[i].conn != nil {
					wg.Add(1)
					go func(c cores) {
						sendMessage(c.conn, master, core, VICTORY, strconv.FormatInt(mq.vote, 10))
						mes, err := waitMessage(c.conn)
						if err != nil || mes == nil {
							fmt.Println("mes", mes)
							fmt.Println("err", err)
						} else if mes.h.operation == ACK {
							list = append(list, c)
						}
						wg.Done()
					}(voteList[i])
				}
			}
			wg.Wait()
			mq.list = list
			go mq.masterKeep()
			fmt.Println("victory list", mq.list)
		} else {
			//通知所有结点胜选者
			//将胜选者设为master
			mq.list[0].Power = core
			mq.vote = victor
			for i := range mq.list {
				if mq.list[i].Id == victor {
					mq.list[i].Power = master
					break
				}
			}
			fmt.Println("defeat list", mq.list)
			//通知支持我的机器最终获胜者
			for i := range voteList {
				if mq.list[i].vote == mq.id {
					wg.Add(1)
					go func(conn net.Conn) {
						sendMessage(conn, master, core, VICTOR, strconv.FormatInt(mq.vote, 10))
						time.Sleep(time.Second)
						wg.Done()
					}(mq.list[i].conn)
				}
			}
			wg.Wait()
			for i := range voteList {
				if voteList[i].Id != victor {
					voteList[i].conn.Close()
					voteList[i].conn = nil
				}
			}
		}
	}
	mq.inVote = false
	mq.Unlock()
}
func (mq *messageQueue) listenVoter(conn net.Conn) {
	data, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("选举节点已断开", conn.RemoteAddr().String())
		conn.Close()
		return
	}
	m, err := analysisMessage([]byte(data))
	if err != nil {
		return
	}
	if mq.vote != mq.id {
		if m.h.operation == VICTOR {
			mq.vote, _ = strconv.ParseInt(m.s, 10, 64)
		} else if m.h.operation == VICTORY {
			if strconv.FormatInt(mq.vote, 10) == m.s {
				mq.master = conn
				feedback(mq.master, core, master, ACK)
				go mq.listenMaster()
			} else {
				feedback(mq.master, core, master, NIL)
			}
		}
	}
}
func (mq *messageQueue) listenMaster() {
	fmt.Println("master结点:", mq.master.RemoteAddr().String())
	mq.lastKeep = time.Now().UnixNano()
	has := true
	go func() {
		for has {
			//每间隔3min检测是否与master链接
			time.Sleep(180 * time.Second)
			if has && time.Now().UnixNano()-mq.lastKeep > int64(180*time.Second) {
				//发起master选举
				mq.voteMaster()
				break
			}
		}
	}()
	for {
		data, err := bufio.NewReader(mq.master).ReadString('\n')
		if err != nil {
			fmt.Println("master节点已断开", mq.master.RemoteAddr().String())
			mq.master.Close()
			has = false
			mq.voteMaster()
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == KEEP {
			mq.lastKeep = time.Now().UnixNano()
			fmt.Println("lastKeep:", mq.lastKeep)
		} else if m.h.operation == AddCore {
			feedback(mq.master, core, master, ACK)
			var c cores
			json.Unmarshal([]byte(m.s), &c)
			c.Addr = addressTransition(mq.master.RemoteAddr().String(), c.Addr)
			mq.Lock()
			mq.list = append(mq.list, c)
			mq.Unlock()
			fmt.Println("添加后的list:", mq.list)
		} else if m.h.operation == DelCore {
			id, err := strconv.ParseInt(m.s, 10, 64)
			if err != nil {
				continue
			}
			mq.Lock()
			list := make([]cores, 0, len(mq.list)-1)
			for i := range mq.list {
				if mq.list[i].Id != id {
					list = append(list, mq.list[i])
				}
			}
			mq.list = list
			mq.Unlock()
			fmt.Println("删除后的list:", mq.list)
		} else if m.h.operation == UPDATE {
			var list []cores
			json.Unmarshal([]byte(m.s), &list)
			for i := range list {
				list[i].Addr = addressTransition(mq.master.RemoteAddr().String(), list[i].Addr)
			}
			mq.Lock()
			mq.list = list
			mq.Unlock()
			fmt.Println("update list:", mq.list)
		}
	}
}
func (mq *messageQueue) addCore(conn net.Conn, id int64, point string) {
	address := strings.SplitN(conn.RemoteAddr().String(), ":", 2)
	c := cores{
		Id:    id,
		Addr:  address[0] + point,
		Power: core,
		conn:  conn,
	}
	s, _ := json.Marshal(c)
	mq.RLock()
	list := mq.list
	mq.RUnlock()
	ss, _ := json.Marshal(list)
	sendMessage(conn, master, core, ACK, string(ss))
	for i := range list {
		if list[i].conn != nil {
			sendMessage(list[i].conn, core, core, AddCore, string(s))
			mes, err := waitMessage(list[i].conn)
			if err != nil || mes == nil || mes.h.operation != ACK {
				fmt.Println(err)
			}
		}
	}
	list = append(list, c)
	mq.Lock()
	mq.list = list
	mq.Unlock()
	fmt.Println("new list", list)
}
func (mq *messageQueue) delCore(id int64) {
	//通知其他结点该结点已断开
	mq.Lock()
	list := make([]cores, 0, len(mq.list)-1)
	for i := range mq.list {
		if mq.list[i].Id != id {
			list = append(list, mq.list[i])
		}
	}
	mq.list = list
	mq.Unlock()
	ids := strconv.FormatInt(id, 10)
	for i := range list {
		if list[i].conn != nil {
			sendMessage(list[i].conn, core, core, DelCore, ids)
		}
	}
}
func (mq *messageQueue) listenCore(conn net.Conn, id int64) {
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("核心节点已断开", conn.RemoteAddr().String())
			conn.Close()
			mq.delCore(id)
			break
		}
		m, err := analysisMessage([]byte(data))
		if err != nil {
			continue
		}
		if m.h.operation == GETPC || m.h.operation == GETPS {
			mq.RLock()
			list := mq.list
			mq.RUnlock()
			wg := sync.WaitGroup{}
			topic := m.s
			receiver := id
			has := false
			num := int64(2 ^ 60)
			for i := range list {
				if list[i].Id != mq.id && list[i].Id != id {
					if list[i].conn != nil {
						wg.Add(1)
						go func() {
							sendMessage(list[i].conn, master, core, m.h.operation, topic)
							mes, err := waitMessage(list[i].conn)
							if !has {
								if err != nil || mes == nil {

								} else if mes.h.operation == ACK {
									receiver = list[i].Id
									has = true
								} else if mes.h.operation == REDIRECT {
									ans, err := strconv.ParseInt(mes.s, 10, 32)
									if !has && err == nil && ans < num {
										receiver = list[i].Id
										num = ans
									}
								}
							}
							wg.Done()
						}()
					}
				}
			}
			wg.Wait()
			addr := ""
			for i := range list {
				if list[i].Id == receiver {
					addr = list[i].Addr
					sendMessage(conn, master, core, ACK, addr)
					break
				}
			}
		}
	}
}
func (mq *messageQueue) listenPro(conn net.Conn, topic string) {
	fmt.Println("生产结点topic:", topic)
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
			if !mq.inVote {
				mq.Lock()
				mq.pc[topic].PushBack(mes)
				mq.Unlock()
				feedback(conn, core, produce, ACK)
			} else {
				feedback(conn, core, produce, NIL)
			}
		}
	}
}
func (mq *messageQueue) listenCon(conn net.Conn, topic string) {
	fmt.Println("消费结点topic:", topic)
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
	fmt.Println("发布结点topic:", topic)
	feedback(conn, core, publish, ACK)
	mq.Lock()
	if _, ok := mq.ps[topic]; !ok {
		mq.ps[topic] = make([]chan *message, 0, 0)
	}
	mq.Unlock()
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
			if !mq.inVote {
				mq.Lock()
				mq.cache.PushBack(info)
				mq.Unlock()
				feedback(conn, core, publish, ACK)
			} else {
				feedback(conn, core, produce, NIL)
			}
		}
	}
}
func (mq *messageQueue) listenSub(conn net.Conn, topic string) {
	fmt.Println("订阅结点topic:", topic)
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
	}()
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			unblocked = false
			conn.Close()
			fmt.Println("订阅节点已断开", conn.RemoteAddr().String())
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
