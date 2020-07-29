package uftp

import (
	"net"
	"time"
	"github.com/willf/bitset"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"sync"
	"github.com/sirupsen/logrus"
	"github.com/AzYet/goutil"
	"strconv"
)

type UftpServer struct {
	FileDataChan chan *DataOut
	conn         *net.UDPConn
	sigInt       chan int
	removalChan  chan string
	pktOutChan   chan *AddrBytesPair
	Query        chan NameResChan
}

func NewUftpServer(ipPort string, log *logrus.Logger, sigInt chan int) (*UftpServer, error) {
	Logger = log
	serverAddr, err := net.ResolveUDPAddr("udp4", ipPort)
	if err != nil {
		Logger.Println("resolve adddr error", err)
		return nil, err
	}
	conn, err := net.ListenUDP("udp4", serverAddr)
	if err != nil {
		Logger.Println("listen udp error", err)
		return nil, err
	}
	Logger.Printf("Listening udp on %v.", ipPort)
	return &UftpServer{make(chan *DataOut), conn, sigInt, make(chan string, 64), make(chan *AddrBytesPair), make(chan NameResChan)}, nil
}

func (s UftpServer) Start() {
	// packet dispatcher, auto create new chan, nil data to delete chan
	addrDataChan := make(chan *AddrBytesPair, 1024 * 128)
	buf := make([]byte, 1024 * 65)
	go func() {
		for {
			n, clientAddr, err := s.conn.ReadFromUDP(buf)
			if err != nil {
				Logger.Println("read udp error", err)
			} else if n > 0 {
				//Logger.Printf("%v bytes received.", n)
				// create new channel only if first byte is Info
				data := make([]byte, n)
				copy(data, buf[:n])
				select {
				case addrDataChan <- &AddrBytesPair{clientAddr, data}:
				default:
					Logger.Warn("exceed max udp data buffer")
				}
			}
		}
	}()
	go func() {
		type  UdpChan struct {
			DataCh chan []byte
			IntCh  chan int
			CTime  time.Time
		}
		udpChanMap := make(map[string]UdpChan)
		tick := goutil.NewTicker(time.Minute, time.Second * 50)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				name, cTime := "none", time.Now()
				for k, v := range udpChanMap {
					if cTime.After(v.CTime) {
						name = k
						cTime = v.CTime
					}
				}
				Logger.WithField("udpChanMap", len(udpChanMap)).WithField("age", time.Since(cTime)).WithField("addr", name).Info("udp server status.")
				if time.Since(cTime) > time.Hour * 2 {
					delete(udpChanMap, name)
					Logger.WithField("age", time.Since(cTime)).WithField("addr", name).Info("delete stale udp channel")
				}
			case <-s.sigInt:
				for _, v := range udpChanMap {
					v.IntCh <- -1
				}
				return
			case id := <-s.removalChan:
			//Logger.Println("delete channel", addr.String())
				delete(udpChanMap, id)
			case ad := <-addrDataChan:
				if c, ok := udpChanMap[ad.Addr.String()]; ok {
					//Logger.Println("dispatch udp frame ", len(data), ad.Addr)
					c.DataCh <- ad.Data
				} else {
					if ad.Data[0] == Info || ad.Data[0] == Get {
						//Logger.Println("new channel", ad.Addr)
						c := make(chan []byte, 64)
						intrpt := make(chan int, 1)
						id := ad.Addr.String() + strconv.FormatInt(time.Now().UnixNano(), 10)
						go s.udpChannelHandler(id, ad.Addr, c, intrpt)
						c <- ad.Data
						udpChanMap[ad.Addr.String()] = UdpChan{c, intrpt, time.Now()}
					}
				}
			}
		}
	}()

	go func() {
		for addrBytes := range s.pktOutChan {
			if _, e := s.conn.WriteToUDP(addrBytes.Data, addrBytes.Addr); e != nil {
				Logger.Printf("send pkt to%v error: %v.", addrBytes.Addr, e)
			}
		}
	}()
}

func ackHandler(cur *int, seqChan chan int, toClientChan chan *AddrBytesPair, ca *net.UDPAddr, sigTerm chan int, nameSum []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Millisecond * AckInterval)
	defer ticker.Stop()
	acks := make([]int, 0, MaxAckLen)
	var preAcks []int
	total := 0
	send := false
	outer: for {
		select {
		case <-sigTerm:
			break outer
		case seq := <-seqChan:
			if seq >= 0 {
				acks = append(acks, seq)
			} else if seq == -1 {
				acks = append([]int{-1}, acks...)
				send = true
			} else if seq == -2 {
				total ++
			}
			if total >= MaxAckLen {
				send = true
			}
		case <-ticker.C:
			if len(acks) > 0 {
				send = true
			}
		}
		lenAck := len(acks)
		if send && lenAck > 0 {
			send = false
			//   0   1    2   3   4   5
			//[Ack][Fno][sum....][N][.....x2]
			bz := make([]byte, md5.Size + 5 + 2 * ( lenAck + len(preAcks)))
			copy(bz, nameSum[:])
			bz[md5.Size] = Ack
			bz[md5.Size + 1] = byte(*cur)
			binary.BigEndian.PutUint16(bz[md5.Size + 2:md5.Size + 4], uint16(total))
			bz[md5.Size + 4] = byte(len(preAcks) + lenAck)

			for k, v := range acks {
				binary.BigEndian.PutUint16(bz[md5.Size + 5 + 2 * k:md5.Size + 7 + 2 * k], uint16(v))
			}
			for k, v := range preAcks {
				binary.BigEndian.PutUint16(bz[md5.Size + 5 + lenAck * 2 + 2 * k:md5.Size + 7 + lenAck * 2 + 2 * k], uint16(v))
			}
			toClientChan <- &AddrBytesPair{ca, bz}
			//Logger.Println("Send out acks", p)
			total = 0
			if acks[0] == -1 {
				preAcks = []int{}
			} else {
				preAcks = acks
			}
			acks = make([]int, 0, MaxAckLen)
		}
	}
	//Logger.Println("ack hadnler exiting")
}

func (s *UftpServer)udpChannelHandler(id string, clientAddr *net.UDPAddr, udpFrameChan chan []byte, informQuitChan chan int) {
	state := WaitingFileInfo
	seqBlockMap := make(map[int][]byte)
	var bs *bitset.BitSet
	var fileName string
	var expectFrag int
	var blockNums []int
	var frags int
	var totalBlocks int
	var nameSum []byte
	var wg = new(sync.WaitGroup)
	fileData := new(bytes.Buffer)
	seqChan := make(chan int)
	cur := 0
	duplicateBlockCount := 0
	sigTerm := make(chan int)
	pktCout := 0
	dataFn := func(udpFrame []byte, dlen int) {
		if len(udpFrame) < dlen + 6 {
			Logger.Println("packet len shorter than dlen.")
			return
		}
		payloadData := udpFrame[6:dlen + 6]
		seq := int(binary.BigEndian.Uint16(udpFrame[2:4]))
		if int(udpFrame[1]) == expectFrag && seq < int(bs.Len()) {
			if bs.Test(uint(seq)) {
				duplicateBlockCount ++
				seqChan <- seq
				if (duplicateBlockCount % 64 == 0) {
					Logger.Println("received 64 duplicate blocks")
					if duplicateBlockCount > 1024 {
						informQuitChan <- -1
					}
				}
			} else {
				seqBlockMap[seq] = payloadData
				bs.Set(uint(seq))
				if bs.All() {
					//Logger.Printf("frag[%v/%v] complete.", udpFrame[1] + 1, frags)
					seqChan <- -1
					for i := 0; i < blockNums[cur]; i++ {
						_, err := fileData.Write(seqBlockMap[i])
						if err != nil {
							Logger.Println("failed to append data to file buff.")
						}
					}
					// should be handled carefully
					if cur == frags - 1 {
						s.FileDataChan <- &DataOut{id, fileName, fileData.Bytes(), false}
						//Logger.Println("File complete")
					} else {
						s.FileDataChan <- &DataOut{id, fileName, fileData.Bytes(), true}
						seqBlockMap = make(map[int][]byte)
					}
					fileData.Reset()
					expectFrag ++
					//Logger.Println("change state to SendingFragFin")
					state = SendingFragFin
				} else {
					seqChan <- seq
					//Logger.Println("progress:", len(seqBlockMap), blockNums[cur])
				}
			}
		} else {
			state = Done
			Logger.Printf("fragNum or seq mismatch, recevied: %v %v, expect: %v %v.", udpFrame[1], seq, expectFrag, bs.Len())
		}
	}
	tick := time.NewTicker(time.Second * 5)
	defer tick.Stop()
	lastPktTime := time.Now()
	for state != Done {
		sel:select {
			case <-tick.C:
				if time.Since(lastPktTime) > time.Minute {
					s.FileDataChan <- &DataOut{id, fileName, nil, true}
					state = Done
				}
			case sig := <-informQuitChan:
			//Logger.Println("quit signal received, abort")
				if sig < 0 {
					s.FileDataChan <- &DataOut{id, fileName, nil, true}
				}
				state = Done
			case udpFrame := <-udpFrameChan:
			//	Logger.Println("state is", state)
			//	Logger.Println("channel receive", len(udpFrame))
				lastPktTime = time.Now()
				pktCout++
				switch state {
				case ReceivingFragData:
					switch udpFrame[0]{
					case Data:
						//Logger.Printf("payload packet: %v,data len:%v.", p.Seq, len(p.Data))
						dlen := int(binary.BigEndian.Uint16(udpFrame[4:6]))
						dataFn(udpFrame, dlen)
					}
					if pktCout % 1024 == 0 {
						r := cur * blockNums[0] + int(bs.Count())
						Logger.Printf("[%v]-[%v/%v+%v]: %.2f%%.", fileName, cur, frags, bs.Count(), float64(r) * 100.0 / float64(totalBlocks))
					}
				case SendingFragFin:
					switch udpFrame[0]{
					case New:
						if int(udpFrame[1]) == expectFrag {
							//Logger.Printf("New Frag: %v.", udpFrame[1])
							//Logger.Println("sending back FileInfo ", Packet{FileInfo{fileName, blockNums, p.Frag}})
							s.pktOutChan <- &AddrBytesPair{clientAddr, append(nameSum, udpFrame...)}
							cur = expectFrag
							if cur < len(blockNums) {
								//Logger.Println("change state to SendingFragAccept")
								state = SendingFragAccept
								duplicateBlockCount = 0
								bs = bitset.New(uint(blockNums[cur]))
							} else {
								//Logger.Println("Final Info received, quit in 3s.", clientAddr)
								state = SendingFragAccept
								go func() {
									time.Sleep(3 * time.Second)
									informQuitChan <- 1
								}()
							}
						}
					default:
						seqChan <- -1
					}
				case WaitingFileInfo:
					switch udpFrame[0]{
					case Get:
						nameLen := int(binary.BigEndian.Uint16(udpFrame[1:3]))
						name := udpFrame[3:3 + nameLen]
						ret := make(chan int, 1)
						Logger.Infof("query %v %v", string(name), clientAddr.String())
						s.Query <- NameResChan{string(name), ret}
						status := <-ret
						bz := make([]byte, 18)
						sum := md5.Sum(name)
						copy(bz[:16], sum[:16])
						bz[16] = Get
						bz[17] = byte(status)
						for i := 0; i < 14; i++ {
							s.pktOutChan <- &AddrBytesPair{clientAddr, bz }
							time.Sleep(time.Second)
						}
						s.removalChan <- clientAddr.String()
						return
					case Info:
						//   0     1             +1
						//[Info][NLen][.......][frags] [N x 2...]
						fileName = string(udpFrame[2:udpFrame[1] + 2])
						frags = int(udpFrame[2 + udpFrame[1]])
						expLen := 3 + int(udpFrame[1]) + int(udpFrame[udpFrame[1] + 2] * 2)
						if expLen != len(udpFrame) {
							Logger.Println("incorrect data len")
							state = Done
							break
						}
						blockNums = make([]int, frags)
						for i := 0; i < frags; i++ {
							blockNums[i] = int(binary.BigEndian.Uint16(udpFrame[3 + int(udpFrame[1]) + i * 2:5 + int(udpFrame[1]) + i * 2]))
						}
						//Logger.Println(blockNums)
						sum := md5.Sum([]byte(fileName))
						nameSum = sum[:]
						//copy(nameSum, s[:])
						for _, i := range blockNums {
							totalBlocks += i
						}
						bs = bitset.New(uint(blockNums[cur]))
						wg.Add(1)
						go ackHandler(&cur, seqChan, s.pktOutChan, clientAddr, sigTerm, nameSum, wg)
						s.FileDataChan <- &DataOut{id, fileName, []byte{}, false}
						//send file info back to confirm reception, till first payload arrival
						Logger.Println(fileName, blockNums)
						//Logger.Println("change state to SendingFragAccept")
						state = SendingFragAccept
						bz := append(nameSum, []byte{Acc, 0}...)
						//Logger.Printf("sending back %v %x", len(bz), bz)
						s.pktOutChan <- &AddrBytesPair{clientAddr, bz }
					default:
						// closing
						Logger.Println("Not a FileInfo Packet with FragNo = 0, discard")
						state = Done
						break
					}
				case SendingFragAccept:
					switch udpFrame[0]{
					//   0    1     2   3    4   5
					//[Data][Fno ][seq....][Dlen....][........]
					case Data:
						dlen := int(binary.BigEndian.Uint16(udpFrame[4:6]))
						if int(udpFrame[1]) == frags && dlen == 0 {
							//Logger.Println("Final Data received, safe to close", clientAddr)
							state = Done
							break sel
						}
						dataFn(udpFrame, dlen)
					default:
						//[Acc][Fno]
						bz := make([]byte, md5.Size + 2)
						copy(bz, nameSum[:])
						bz[md5.Size] = Acc
						bz[md5.Size + 1] = byte(cur)
						s.pktOutChan <- &AddrBytesPair{clientAddr, bz}
					}
				default:
					Logger.Println("unhandled state?", state)
				}
				seqChan <- -2
			}
	}
	sigTerm <- 1
	s.removalChan <- clientAddr.String()
	wg.Wait()
	//Logger.Println("channel closing...", clientAddr.String())
}
