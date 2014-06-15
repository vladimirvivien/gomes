package gomes

import (
	"fmt"
	"path"
	"sync"
	"strings"
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
	prefix := MESOS_SCHEDULER_PREFIX + "(" +  cntStr + ")"
	value := prefix + "@" + addr
	id := schedProcID{prefix:prefix,value:value}
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
	controlQ chan int32
}

// newSchedHttpProcess creates and starts htttp process.
func newSchedulerProcess (eventQ chan<- interface{}) (*schedulerProcess, error) {
	if eventQ == nil {
		return nil, fmt.Errorf("SchedulerProcess - eventQ parameber cannot be nil.")
	}

	serv := &http.Server {
		Addr: ":0",
	}

	proc := &schedulerProcess{
		server:serv, 
		eventMsgQ:eventQ,
		controlQ: make(chan int32),
	}
	
	return proc, nil
}

func (proc *schedulerProcess) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
	code := http.StatusAccepted
	var comment string = ""

	// decompose incoming request path of expected form: 
	// /scheduler(?)/mesos.internal.<MessageName>
	_,internalName := path.Split(req.URL.Path)
	messageType := strings.Split(internalName,".")[2] // last index is messageType.

	data, err := ioutil.ReadAll(req.Body)
	if err != nil{
		code = http.StatusBadRequest
		comment = "Request body missing."
	}
	defer req.Body.Close()

	// dispatch msg based on type
	var msg proto.Message
	switch messageType {
		case "FrameworkRegisteredMessage":
			msg = new (mesos.FrameworkRegisteredMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = "Error unmashalling FrameworkRegisteredMessage"
			}
			
		default:
			code = http.StatusBadRequest
			comment = messageType +  " unrecognized."
	}
	
	proc.eventMsgQ <- msg

	rsp.WriteHeader(code)
	if comment != ""{
		fmt.Fprintln(rsp, comment)
	}
}


// start Starts an http process to listen to incoming events from Mesos.
func (proc *schedulerProcess) start() {	
	proc.server.Addr = fmt.Sprintf("%s:%d", localIP4String(), nextTcpPort())
	proc.processId = newSchedProcID(proc.server.Addr)

	// register listners
	procPath := fmt.Sprintf("/%s/%s%s", 
		proc.processId.prefix, 
		MESOS_INTERNAL_PREFIX,
		FRAMEWORK_REGD_MESSAGE)
	http.Handle(procPath, proc)
	fmt.Println ("*** Registered handler for path:",procPath)

	go proc.server.ListenAndServe()
}