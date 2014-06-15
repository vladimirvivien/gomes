package gomes

import (
	"net"
	"fmt"
    "net/http"
    "net/url"
    "bytes"
    "time"
    _"log"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"
)

const (
	MESOS_INTERNAL_PREFIX	= "mesos.internal."
	MESOS_SCHEDULER_PREFIX	= "scheduler"
	HTTP_SCHEDULER_PORT		= 51515
	HTTP_SCHEME 			= "http"
	HTTP_POST_METHOD		= "POST"
	HTTP_MASTER_PREFIX		= "master"
	HTTP_LIBPROC_PREFIX 	= "libprocess/"
	HTTP_CONTENT_TYPE		= "application/x-protobuf"

	MESSAGE_REG_FRAMEWORK 	= "RegisterFrameworkMessage"

)


type address string
func (addr address) AsFullHttpURL(path string) (*url.URL, error){
	return url.Parse(HTTP_SCHEME + "://" + string(addr) + "/" + path)
}
func (addr address) AsHttpURL()(*url.URL, error){
	return addr.AsFullHttpURL("")
}

type masterClient struct {
	address address
	httpClient http.Client
}

func newMasterClient(master string) *masterClient {
	return &masterClient{
		address:address(master), 
		httpClient:http.Client{
			Transport : &http.Transport {
				Dial: func(netw, addr string) (net.Conn, error) {
					c, err := net.DialTimeout(netw, addr, time.Second * 17)
					if err != nil {
						return nil, err
					}
					return c, nil
				},
				DisableCompression : true,
			},
		},
	}
}

func (client *masterClient) RegisterFramework(schedId schedProcID, framework *mesos.FrameworkInfo) (error){
	// prepare registration data
	regMsg := &mesos.RegisterFrameworkMessage{Framework:framework}
	return client.send (schedId, buildReqPath(MESSAGE_REG_FRAMEWORK), regMsg)
}

func (client *masterClient) send (from schedProcID, reqPath string, msg proto.Message) error {
	u, err := client.address.AsHttpURL()
	if(err != nil){
		return err
	}
	u.Path = reqPath

	data, err := proto.Marshal(msg)
	if (err != nil){
		return err
	}
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", from.value)
	rsp, err := client.httpClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Master did not accept request %s.  Returned status %s.", u.String(), rsp.Status)
	}
	return nil
}

func buildReqPath(message string) string {
	return "/"+ HTTP_MASTER_PREFIX + "/" + MESOS_INTERNAL_PREFIX + message
}

// a generic send function. Will build message path based on msg type.
// func (client *masterClient) send(msg proto.ProtoMessage) (error) {
// }
