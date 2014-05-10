package gomes

import (
	"net"
    "net/http"
    "net/http/httputil"
    "net/url"
    "bytes"
    "time"
    "log"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"
)

const (
	HTTP_SCHEME 		= "http"
	REG_FRAMEWORK_CMD 	= "/mesos.internal.RegisterFrameworkMessage"
	LIBPROC_PREFIX   = "libprocess/"
)

type PID string
func (pid PID) AsURL()(*url.URL, error){
	return url.Parse(HTTP_SCHEME + "://" + string(pid))
}

type MesosMasterClient interface {
	RegisterFramework(PID,mesos.FrameworkInfo)
}

type masterClientStruct struct {
	Pid PID
	httpClient http.Client
}

func NewMasterClient(pid PID) *masterClientStruct {
	return &masterClientStruct{
		Pid:pid, 
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

func (client *masterClientStruct) RegisterFramework(frameWorkPid PID, regMsg mesos.RegisterFrameworkMessage) (error){
	u, err := client.Pid.AsURL()
	if(err != nil){
		return err
	}
	// build Master path
	u.Path = u.User.Username() + REG_FRAMEWORK_CMD

	// prepare request
	log.Println (regMsg.String())
	data, err := proto.Marshal(&regMsg)
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", string(frameWorkPid))
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