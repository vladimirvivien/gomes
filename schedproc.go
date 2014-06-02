package gomes

import (
	"fmt"
	"path"
	"sync"
	"strconv"
	"net/url"
	"net/http"
	"io/ioutil"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    "code.google.com/p/goprotobuf/proto"

)

var schedIdMutex = new(sync.Mutex)
var schedIdCounter = 0
type schedProcID struct {
	prefix string
	value string
}
func newSchedProcID(addr string) schedProcID {
	cntStr := strconv.Itoa(schedIdCounter)
	value := MESOS_SCHEDULER_PREFIX + "(" +  cntStr + ")@" + addr
	id := schedProcID{prefix:MESOS_SCHEDULER_PREFIX,value:value}
	schedIdMutex.Lock()
	schedIdCounter = schedIdCounter + 1
	schedIdMutex.Unlock()
	return id
}
func (id *schedProcID) asURL() (*url.URL, error){
	return url.Parse("http://"+id.value)
}

/*
SchedHttpProcess manages http requests from the connected master.
It wraps the standard Http Server.
*/
type schedulerProcess struct {
	server *http.Server
	processId schedProcID
	eventMsgQ chan<- interface{}
}

// newSchedHttpProcess creates and starts htttp process.
func newSchedulerProcess (addr string, eventQ chan<- interface{}) (*schedulerProcess, error) {
	if eventQ == nil {
		return nil, fmt.Errorf("SchedulerProcess - eventQ parameber cannot be nil.")
	}

	serv := &http.Server {
		Addr: addr,
	}

	proc := &schedulerProcess{
		server:serv, 
		processId:newSchedProcID(addr),
		eventMsgQ:eventQ,
	}
	
	return proc, nil
}

func (proc *schedulerProcess) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
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