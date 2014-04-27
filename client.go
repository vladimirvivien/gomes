package ionos

import (
    "net/http"
    "net/url"
    "bytes"
    mesos "github.com/vladimirvivien/ionos/mesosproto"
    "code.google.com/p/goprotobuf/proto"
)

const (
	HTTP_SCHEME 		= "http"
	REG_FRAMEWORK_CMD 	= "/mesos.internal.RegisterFrameworkMessage"
	USER_AGENT_PREFIX   = "libprocess/"
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
	return &masterClientStruct{Pid:pid, httpClient:http.Client{}}
}

func (client *masterClientStruct) RegisterFramework(frameWorkPid PID, info mesos.FrameworkInfo) (error){
	u, err := client.Pid.AsURL()
	if(err != nil){
		return err
	}
	// build Master path
	u.Path = REG_FRAMEWORK_CMD

	// prepare request
	data, err := proto.Marshal(&info)
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(data))
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("User-Agent", USER_AGENT_PREFIX + string(frameWorkPid))

	client.httpClient.Do(req)

	return nil
}