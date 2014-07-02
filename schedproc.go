package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
)

var schedIdMutex = new(sync.Mutex)
var schedIdCounter = 0

type schedProcID struct {
	prefix string
	value  string
}

func newSchedProcID(addr string) schedProcID {
	cntStr := strconv.Itoa(schedIdCounter)
	prefix := MESOS_SCHEDULER_PREFIX + "(" + cntStr + ")"
	value := prefix + "@" + addr
	id := schedProcID{prefix: prefix, value: value}
	schedIdMutex.Lock()
	schedIdCounter = schedIdCounter + 1
	schedIdMutex.Unlock()
	return id
}
func (id *schedProcID) asURL() (*url.URL, error) {
	return url.Parse("http://" + id.value)
}

/*
SchedHttpProcess manages http requests from the connected master.
It wraps the standard Http Server.
*/
type schedulerProcess struct {
	listener  net.Listener
	server    *http.Server
	processId schedProcID
	eventMsgQ chan<- interface{}
	controlQ  chan int32
}

// newSchedHttpProcess creates and starts htttp process.
func newSchedulerProcess(eventQ chan<- interface{}) (*schedulerProcess, error) {
	if eventQ == nil {
		return nil, fmt.Errorf("SchedulerProcess - eventQ parameber cannot be nil.")
	}

	serv := &http.Server{
	//Addr: ":0",
	//ReadTimeout:nil,
	//WriteTimeout:
	//ConnState : nil,
	}

	proc := &schedulerProcess{
		server:    serv,
		eventMsgQ: eventQ,
		controlQ:  make(chan int32),
	}

	return proc, nil
}

// start Starts the internal http process to listen to incoming events from Master.
func (proc *schedulerProcess) start() error {
	addr := fmt.Sprintf("%s:%d", localIP4String(), nextTcpPort())
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	proc.listener = listener
	proc.processId = newSchedProcID(addr)
	proc.registerEventHandlers()

	// launch internal http.server
	go func(lis net.Listener) {
		err := proc.server.Serve(lis)
		// TODO hack: if error.Op = 'accept' assume connection got closed
		//      and server.listener stopped blocking on Accept() operation.
		val, ok := err.(*net.OpError)
		if !ok || val.Op != "accept" {
			proc.eventMsgQ <- err
		}
	}(proc.listener)

	// ping proc.server listening at proc.listener.Addr()
	rsp, err := http.Get("http://" + proc.listener.Addr().String() + "/isalive")
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		return NewMesosError("SchedProc.server ping failed. Server may not be running.")
	}
	return nil
}

// stop Stops the Scheduler process and internal server.
func (proc *schedulerProcess) stop() error {
	//TODO Needs a better way than this.
	err := proc.listener.Close()
	if err != nil {
		return err
	}
	return nil
}

// registerEventHandlers Registers http handlers for Mesos master events.
func (proc *schedulerProcess) registerEventHandlers() {
	//TODO hack: only way to clear default mux.
	//     fix - maybe use custom mux
	http.DefaultServeMux = http.NewServeMux()
	http.HandleFunc("/isalive", func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusOK)
	})

	http.Handle(makeProcEventPath(proc, FRAMEWORK_REGISTERED_EVENT), proc)
	http.Handle(makeProcEventPath(proc, FRAMEWORK_REREGISTERED_EVENT), proc)
	http.Handle(makeProcEventPath(proc, RESOURCE_OFFERS_EVENT), proc)
	http.Handle(makeProcEventPath(proc, RESCIND_OFFER_EVENT), proc)
	http.Handle(makeProcEventPath(proc, STATUS_UPDATE_EVENT), proc)
	http.Handle(makeProcEventPath(proc, FRAMEWORK_MESSAGE_EVENT), proc)
	http.Handle(makeProcEventPath(proc, LOST_SLAVE_EVENT), proc)
}

func (proc *schedulerProcess) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
	code := http.StatusAccepted
	var comment string = ""

	// decompose incoming request path of form:
	// /scheduler(?)/mesos.internal.<MessageTypeNamee>
	_, internalName := path.Split(req.URL.Path)      // returns mesos.internal.<MessageTypeName>
	messageParts := strings.Split(internalName, ".") // last index is messageType.

	// if request path is badly formed
	if len(messageParts) != 3 {
		err := NewMesosError("Event posted by master is malformed:" + req.URL.Path)
		proc.eventMsgQ <- err
		code = http.StatusBadRequest
		comment = "Request path malformed."
	} else {
		messageType := messageParts[2]

		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			code = http.StatusBadRequest
			comment = "Request body missing."
		}
		defer req.Body.Close()

		// dispatch msg based on type
		var msg proto.Message
		switch messageType {
		case FRAMEWORK_REGISTERED_EVENT:
			msg = new(mesos.FrameworkRegisteredMessage)
			err = proto.Unmarshal(data, msg)

		case FRAMEWORK_REREGISTERED_EVENT:
			msg = new(mesos.FrameworkReregisteredMessage)
			err = proto.Unmarshal(data, msg)

		case RESOURCE_OFFERS_EVENT:
			msg = new(mesos.ResourceOffersMessage)
			err = proto.Unmarshal(data, msg)

		case RESCIND_OFFER_EVENT:
			msg = new(mesos.RescindResourceOfferMessage)
			err = proto.Unmarshal(data, msg)

		case STATUS_UPDATE_EVENT:
			msg = new(mesos.StatusUpdateMessage)
			err = proto.Unmarshal(data, msg)

		case FRAMEWORK_MESSAGE_EVENT:
			msg = new(mesos.ExecutorToFrameworkMessage)
			err = proto.Unmarshal(data, msg)

		case LOST_SLAVE_EVENT:
			msg = new(mesos.LostSlaveMessage)
			err = proto.Unmarshal(data, msg)

		default:
			err = fmt.Errorf("Unable to parse event from master: %s unrecognized.", messageType)
			code = http.StatusBadRequest
			comment = err.Error()
		}

		if err != nil {
			code = http.StatusBadRequest
			comment = fmt.Sprintf("Error unmashalling %s: %s", messageType, err.Error())
			proc.eventMsgQ <- NewMesosError(comment)
		} else {
			proc.eventMsgQ <- msg
		}
	}

	rsp.WriteHeader(code)
	if comment != "" {
		fmt.Fprintln(rsp, comment)
	}
}

func makeProcEventPath(proc *schedulerProcess, eventName string) string {
	return fmt.Sprintf("/%s/%s%s", proc.processId.prefix, MESOS_INTERNAL_PREFIX, eventName)
}
