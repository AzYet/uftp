package uftp

import (
	"net"
	"time"
	"github.com/sirupsen/logrus"
)

const AckInterval time.Duration = 1000        //milliseconds
const MaxAckLen int = 16                //  xx seq numbers per ack

var Logger = logrus.New()
//var Logger = log.New(os.Stdout, "[uftp] ", log.LstdFlags)

type DataOut struct {
	ID       string
	FileName string
	Data     []byte
	More     bool
}
type NameResChan struct {
	Name string
	Ret  chan int
}

type AddrBytesPair struct {
	Addr *net.UDPAddr
	Data []byte
}

const (
	Info byte = iota
	New
	Acc
	Data
	Ack
	Get
)

type ChannelState int

const (
	WaitingFileInfo ChannelState = iota
	SendingFragAccept
	ReceivingFragData
	SendingFragFin
	Done
)

func (s ChannelState)String() string {
	switch s{
	case WaitingFileInfo:
		return "WaitingFileInfo"
	case SendingFragAccept:
		return "SendingFragAccept"
	case ReceivingFragData:
		return "ReceivingFragData"
	case SendingFragFin:
		return "SendingFragAccept"
	case Done:
		return "Done"
	default:
		return "undefined"
	}
}

