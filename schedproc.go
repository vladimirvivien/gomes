package gomes

import (
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

/*
SchedHttpProcess manages http requests from the connected master.
It wraps the standard Http Server.
*/
type schedulerProcess struct {
	server *http.Server
	processId string
	eventMsgQ chan interface{}
}

// newSchedHttpProcess creates and starts htttp process.
func newSchedulerProcess (addr string, eventQ chan interface{}) *schedulerProcess {
	serv := &http.Server {
		Addr: addr,
	}

	pid := string(newID("scheduler")) + addr

	proc := &schedulerProcess{
		server:serv, 
		processId:pid,
		eventMsgQ:eventQ,
	}
	
	return proc
}

func (proc *schedulerProcess) ServeHTTP (rsp http.ResponseWriter, req *http.Request) {
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

	// dispatch msg based on type
	var msg proto.Message
	switch reqType {
		case "FrameworkRegisteredMessage":
			msg = new (mesos.FrameworkRegisteredMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = "Error unmashalling FrameworkRegisteredMessage"
			}
			
		default:
			code = http.StatusBadRequest
			comment = reqType +  " unrecognized."
	}
	
	proc.eventMsgQ <- msg

	rsp.WriteHeader(code)
	if comment != ""{
		fmt.Fprintln(rsp, comment)
	}
}


// Starts the http process
func (proc *schedulerProcess) start() {
	http.Handle("/scheduler/FrameworkRegistered", proc)
	go proc.server.ListenAndServe()
}