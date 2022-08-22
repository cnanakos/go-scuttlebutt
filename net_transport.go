package scuttlebutt

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	metrics "github.com/armon/go-metrics"
)

const (
	udpPacketBufSize = 65535
	udpRecvBufSize   = 2 * 1024 * 1024
)

type udpPacket struct {
	Buf  []byte
	From *net.UDPAddr
}

type netTransport struct {
	config      *Config
	packetChan  chan *udpPacket
	logger      *log.Logger
	wg          sync.WaitGroup
	udpListener *net.UDPConn
	closing     int32
}

func newNetTransport(config *Config, logger *log.Logger) (*netTransport, error) {
	var ok bool

	nt := &netTransport{
		config:     config,
		packetChan: make(chan *udpPacket),
		logger:     logger,
	}

	s, err := net.ResolveUDPAddr("udp4", config.ListenAddr)
	if err != nil {
		return nil, err
	}

	nt.udpListener, err = net.ListenUDP("udp4", s)
	if err != nil {
		return nil, err
	}

	defer func() {
		if !ok {
			nt.udpListener.Close()
		}
	}()

	if err := setUDPRecvBuf(nt.udpListener); err != nil {
		return nil, fmt.Errorf("failed to resize UDP buffer: %v", err)
	}

	nt.wg.Add(1)
	go nt.udpListen()

	ok = true
	return nt, nil
}

func (nt *netTransport) packetCh() <-chan *udpPacket {
	return nt.packetChan
}

func (nt *netTransport) isClosing() int32 {
	return atomic.LoadInt32(&nt.closing)
}

func (nt *netTransport) udpListen() {
	defer nt.wg.Done()
	for {
		buf := make([]byte, udpPacketBufSize)
		n, addr, err := nt.udpListener.ReadFromUDP(buf)
		if err != nil {
			if s := atomic.LoadInt32(&nt.closing); s == 1 {
				break
			}
			nt.logger.Printf("[ERR] listener error: %v\n", err)
			continue
		}

		if n < 1 {
			nt.logger.Printf("[ERR] UDP packet too short: %d %s\n", n, addr.String())
			continue
		}

		metrics.IncrCounter([]string{"scuttlebutt", "udp", "received"}, float32(n))

		nt.packetChan <- &udpPacket{
			Buf:  buf[:n],
			From: addr,
		}
	}
}

func (nt *netTransport) writeToUDP(buf []byte, addr *net.UDPAddr) (int, error) {
	metrics.IncrCounter([]string{"scuttlebutt", "udp", "sent"}, float32(len(buf)))
	n, err := nt.udpListener.WriteToUDP(buf, addr)
	return n, err
}

func (nt *netTransport) shutdown() error {
	atomic.StoreInt32(&nt.closing, 1)
	nt.udpListener.Close()
	nt.wg.Wait()
	return nil
}
