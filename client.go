package gomes

import (
	"net"
    "net/http"
    "net/http/httputil"
    "net/url"
    "bytes"
    "time"
    "log"
    "os"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"
)

const (
	MESOS_INTERNAL_PREFIX	= "mesos.internal."
	HTTP_SCHEME 			= "http"
	HTTP_POST_METHOD		= "POST"
	HTTP_MASTER_PREFIX		= "master"
	HTTP_LIBPROC_PREFIX 	= "libprocess/"
	HTTP_CONTENT_TYPE		= "application/x-protobuf"

	MESSAGE_REG_FRAMEWORK 	= "RegisterFrameworkMessage"

)


type address string
func (addr address) AsURL()(*url.URL, error){
	return url.Parse(HTTP_SCHEME + "://" + string(addr))
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

func (client *masterClient) RegisterFramework(schedulerId ID, framework *mesos.FrameworkInfo) (error){
	u, err := client.address.AsURL()
	if(err != nil){
		return err
	}
	// build Master path
	u.Path = buildReqPath(MESSAGE_REG_FRAMEWORK)

	// prepare registration data
	regMsg := &mesos.RegisterFrameworkMessage{Framework:framework}
	data, err := proto.Marshal(regMsg)
	if (err != nil){
		return err
	}

	// prepare HTTP requrest
	localHost,err := os.Hostname()
	if (err != nil){
		return nil
	}
	localHost = string(schedulerId) + localHost + ":" + "5050"

	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", localHost)

	_, err = client.httpClient.Do(req)
	
	if err != nil {
		return err
	}

	return nil
}

func buildReqPath(message string) string {
	return "/"+ HTTP_MASTER_PREFIX + "/" + MESOS_INTERNAL_PREFIX + message
}

// a generic send function. Will build message path based on msg type.
// func (client *masterClient) send(msg proto.ProtoMessage) (error) {
// }

func dumpReq (req *http.Request) {
	out, _ := httputil.DumpRequestOut(req, false)
	log.Println ("Request Body:\n", string(out))
}

func dumpRsp (rsp *http.Response) {
	out, _ := httputil.DumpResponse(rsp, false)
	log.Println ("Response Body:\n", string(out))	
}