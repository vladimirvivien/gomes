package gomes

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

type address string

func (addr address) AsFullHttpURL(path string) (*url.URL, error) {
	return &url.URL{
		Scheme: HTTP_SCHEME,
		Host:   string(addr),
		Path:   path,
	}, nil
}

func (addr address) AsHttpURL() (*url.URL, error) {
	return &url.URL{
		Scheme: HTTP_SCHEME,
		Host:   string(addr),
	}, nil
}

func localIP4String() string {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		switch addr.(type) {
		case *net.IPNet:
			ip := addr.(*net.IPNet)
			if !ip.IP.IsLoopback() && ip.IP.To4() != nil {
				return ip.String()[:strings.LastIndex(ip.String(), "/")]
			}
		}
	}
	return ""
}

func nextTcpPort() int {
	l, err := net.Listen("tcp4", ":0")
	defer l.Close()
	addr := l.Addr().String()
	port, err := strconv.Atoi(addr[strings.LastIndex(addr, ":")+1:])
	if err != nil {
		return 0
	}
	return port
}
