package gomes

import (
	"os"
	"fmt"
	"path"
	"strconv"
	"net/http"
	"math/rand"
	"io/ioutil"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"

)

type ID string
func newID(prefix string) ID {
	return ID(prefix + "(" + strconv.Itoa(rand.Intn(5)) + ")")
}


var schedHttpHandler = func (rsp http.ResponseWriter, req *http.Request) {
	code := http.StatusAccepted
	var comment string = ""

	// decompose incoming request
	_,reqType := path.Split(req.URL.Path)

	data, err := ioutil.ReadAll(req.Body)
	if err != nil{
		code = http.StatusBadRequest
		comment = "Request body missing."
	}
	defer req.Body.Close()

	switch reqType {
		case "FrameworkRegisteredMessage":
			msg := new (mesos.FrameworkRegisteredMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = "Error unmashalling FrameworkRegisteredMessage"
			}
		default:
			code = http.StatusBadRequest
			comment = reqType +  " unrecognized."
	}
	// 
	//<dispatch internal handler here ...> {} 
	//
	rsp.WriteHeader(code)
	if comment != ""{
		fmt.Fprintln(rsp, comment)
	}
}

/*
SchedHttpProcess manages http requests from the connected master.
It wraps the standard Http Server.
*/
type schedHttpProcess struct {
	server *http.Server
	processId string
	httpHandler func (http.ResponseWriter, *http.Request) 
}

// newSchedHttpProcess creates and starts htttp process.
func newSchedHttpProcess (port int) *schedHttpProcess {
	portStr := strconv.Itoa(port)
	serv := &http.Server {
		Addr: ":" + portStr,
	}

	localHost,err := os.Hostname()
	if (err != nil){
		localHost = "localhost"
	}

	pid := string(newID("scheduler")) + localHost + ":" + portStr

	return &schedHttpProcess{
		server:serv, 
		processId:pid,
		httpHandler:schedHttpHandler,
	}
}

// Starts the http process
func (proc *schedHttpProcess) start() {
	http.HandleFunc("/scheduler/FrameworkRegistered", proc.httpHandler)
	go proc.server.ListenAndServe()
}