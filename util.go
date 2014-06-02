package gomes

import (
	"net"
	"strings"
)

func hostIP4AsString() string {
    addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		switch addr.(type){
		case *net.IPNet:
			ip :=  addr.(*net.IPNet)
			if!ip.IP.IsLoopback() && ip.IP.To4() != nil {
		    	return ip.String()[:strings.LastIndex(ip.String(),"/")]
		    }
		}
	}
	return ""
}

func nextAvailbleTcpPort() int {
	return 0
}