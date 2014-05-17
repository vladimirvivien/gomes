package gomes

import (
	"net"
    "net/http"
    "net/http/httputil"
    "net/url"
    "math/rand"
    "bytes"
    "time"
    "log"
    "os"
    "strconv"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"
)

const (
	HTTP_SCHEME 		= "http"
	HTTP_POST_METHOD	= "POST"
	HTTP_MASTER_PREFIX	= "master"
	HTTP_REG_PATH 		= "/mesos.internal.RegisterFrameworkMessage"
	HTTP_LIBPROC_PREFIX = "libprocess/"
)

type ID string
func newID(prefix string) ID {
	return ID(prefix + "(" + strconv.Itoa(rand.Intn(5)) + ")")
}

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

func (client *masterClient) registerFramework(schedId ID, framework *mesos.FrameworkInfo) (error){
	u, err := client.address.AsURL()
	if(err != nil){
		return err
	}
	// build Master path
	u.Path = HTTP_MASTER_PREFIX + HTTP_REG_PATH

	// prepare registration data
	data, err := proto.Marshal(framework)
	if (err != nil){
		return err
	}

	// prepare HTTP requrest
	localHost,err := os.Hostname()
	if (err != nil){
		return nil
	}
	localHost = string(schedId) + localHost + ":" + "5050"

	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", localHost)
	log.Println ("Sending RegisterFramework request to ", u.String())
	dumpReq(req)

	rsp, err := client.httpClient.Do(req)
	dumpRsp(rsp)
	
	if err != nil {
		return err
	}

	return nil
}

func dumpReq (req *http.Request) {
	out, _ := httputil.DumpRequestOut(req, false)
	log.Println ("Request Body:\n", string(out))
}

func dumpRsp (rsp *http.Response) {
	out, _ := httputil.DumpResponse(rsp, false)
	log.Println ("Response Body:\n", string(out))	
}