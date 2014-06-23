package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"io/ioutil"
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
		Addr: ":0",
	}

	proc := &schedulerProcess{
		server:    serv,
		eventMsgQ: eventQ,
		controlQ:  make(chan int32),
	}

	return proc, nil
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
			if err != nil {
				code = http.StatusBadRequest
				comment = fmt.Sprintf("Error unmashalling %s: %s", FRAMEWORK_REGISTERED_EVENT, err.Error())
			}

		case FRAMEWORK_REREGISTERED_EVENT:
			msg = new(mesos.FrameworkReregisteredMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = fmt.Sprintf("Error unmashalling %s: %s", FRAMEWORK_REREGISTERED_EVENT, err.Error())
			}

		case RESOURCE_OFFERS_EVENT:
			msg = new(mesos.ResourceOffersMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = fmt.Sprint("Error unmashalling %s: %s", RESOURCE_OFFERS_EVENT, err.Error())
			}

		case RESCIND_OFFER_EVENT:
			msg = new(mesos.RescindResourceOfferMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = fmt.Sprintf("Error unmashalling %s: %s", RESCIND_OFFER_EVENT, err.Error())
			}
		case STATUS_UPDATE_EVENT:
			msg = new(mesos.StatusUpdateMessage)
			err = proto.Unmarshal(data, msg)
			if err != nil {
				code = http.StatusBadRequest
				comment = fmt.Sprintf("Error unmashalling %s: %s", STATUS_UPDATE_EVENT, err.Error())
			}
		default:
			err = fmt.Errorf("Unable to parse event from master")
			code = http.StatusBadRequest
			comment = err.Error() + ": " + messageType + " unrecognized."
		}

		if err == nil && code == http.StatusAccepted {
			proc.eventMsgQ <- msg
		} else {
			proc.eventMsgQ <- NewMesosError(comment)
		}
	}

	rsp.WriteHeader(code)
	if comment != "" {
		fmt.Fprintln(rsp, comment)
	}
}

// start Starts the internal http process to listen to incoming events from Master.
func (proc *schedulerProcess) start() {
	proc.server.Addr = fmt.Sprintf("%s:%d", localIP4String(), nextTcpPort())
	proc.processId = newSchedProcID(proc.server.Addr)
	proc.registerEventHandlers()
	go func() {
		err := proc.server.ListenAndServe()
		proc.eventMsgQ <- err
	}()
}

// registerEventHandlers Registers http handlers for Mesos master events.
func (proc *schedulerProcess) registerEventHandlers() {
	http.Handle(makeProcEventPath(proc, FRAMEWORK_REGISTERED_EVENT), proc)
	http.Handle(makeProcEventPath(proc, FRAMEWORK_REREGISTERED_EVENT), proc)
	http.Handle(makeProcEventPath(proc, RESOURCE_OFFERS_EVENT), proc)
}

func makeProcEventPath(proc *schedulerProcess, eventName string) string {
	return fmt.Sprintf("/%s/%s%s", proc.processId.prefix, MESOS_INTERNAL_PREFIX, eventName)
}
