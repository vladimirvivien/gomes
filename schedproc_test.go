package gomes

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestNewSchedID(t *testing.T) {
	re1 := regexp.MustCompile(`^[a-z]+\(\d\)@.*$`)
	id1 := newSchedProcID(":5000")
	if !re1.MatchString(string(id1.value)) {
		t.Error("SchedID not generated properly:", id1.value)
	}

	id2 := newSchedProcID(":6000")
	re2 := regexp.MustCompile(`^[a-z]+\(\d\)@.*$`)
	if !re2.MatchString(string(id2.value)) {
		t.Error("SchedID not generated properly.  Expected prefix scheduler(2):", id1.value)
	}
	id3 := newSchedProcID(":7000")
	re3 := regexp.MustCompile(`^[a-z]+\(\d\)$`)
	if !re3.MatchString(id3.prefix) {
		t.Error("SchedID has invalid prefix:", id3.prefix)
	}
}

func TestNewFullSchedID(t *testing.T) {
	re1 := regexp.MustCompile(`scheduler\(\d\)@machine1:4040`)
	id1 := newSchedProcID("machine1:4040")
	if !re1.MatchString(id1.value) {
		t.Errorf("Expecting SchedID [%s], but got [%s]", `scheduler\(\d\)@machine1:4040`, id1.value)
	}
}

func TestSchedProcCreation(t *testing.T) {
	proc, err := newSchedulerProcess(make(chan interface{}))
	if err != nil {
		t.Fatal(err)
	}
	if proc.server == nil {
		t.Error("SchedHttpProcess missing server")
	}
}

func TestScheProcError(t *testing.T) {
	eventQ := make(chan interface{})
	go func() {
		msg := <-eventQ
		if val, ok := msg.(error); !ok {
			t.Fatalf("Expected message of type error, but got %T", msg)
		} else {
			log.Println("*** Error in Q", val, "***")
		}
	}()

	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}
	req := buildHttpRequest(t, "FrameworkRegisteredMessage", nil)
	resp := httptest.NewRecorder()
	proc.ServeHTTP(resp, req)
}

func TestFrameworkRegisteredMessage(t *testing.T) {
	// setup chanel to receive unmarshalled message
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ {
			val, ok := msg.(*mesos.FrameworkRegisteredMessage)
			if !ok {
				t.Fatal("Failed to receive msg of type FrameworkRegisteredMessage")
			}
			if val.FrameworkId.GetValue() != "test-framework-1" {
				t.Fatal("Expected FrameworkRegisteredMessage.Framework.Id.Value not found.")
			}
			if val.MasterInfo.GetId() != "master-1" {
				t.Fatal("Expected FrameworkRegisteredMessage.Master.Id not found.")
			}
		}
	}()

	// Simulate FramworkRegisteredMessage request from master.
	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}
	msg := &mesos.FrameworkRegisteredMessage{
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:   proto.String("master-1"),
			Ip:   proto.Uint32(123456),
			Port: proto.Uint32(12345),
		},
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Unable to marshal FrameworkRegisteredMessage, %v", err)
	}

	req := buildHttpRequest(t, "FrameworkRegisteredMessage", data)
	resp := httptest.NewRecorder()

	// ServeHTTP will unmarshal msg and place on passed channel (above)
	proc.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}
}

func TestFrameworkReRegisteredMessage(t *testing.T) {
	// setup chanel to receive unmarshalled message
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ {
			val, ok := msg.(*mesos.FrameworkReregisteredMessage)
			if !ok {
				t.Fatal("Failed to receive msg of type FrameworkReregisteredMessage")
			}
			if val.MasterInfo.GetId() != "master-1" {
				t.Fatal("Expected FrameworkRegisteredMessage.Master.Id not found. Got", val)
			}
		}
	}()

	// Simulate FramworkReregisteredMessage request from master.
	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}
	msg := &mesos.FrameworkReregisteredMessage{
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:   proto.String("master-1"),
			Ip:   proto.Uint32(123456),
			Port: proto.Uint32(12345),
		},
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Unable to marshal FrameworkReregisteredMessage, %v", err)
	}

	req := buildHttpRequest(t, "FrameworkReregisteredMessage", data)
	resp := httptest.NewRecorder()

	// ServeHTTP will unmarshal msg and place on passed channel (above)
	proc.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}
}

func TestResourceOffersMessage(t *testing.T) {
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ {
			val, ok := msg.(*mesos.ResourceOffersMessage)
			if !ok {
				t.Fatal("Failed to receive msg of type ResourceOffersMessage")
			}
			if len(val.Offers) != 1 {
				t.Fatal("SchedProc not receiving ResourceOffersMessage properly. ")
			}
		}
	}()

	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}

	msg := &mesos.ResourceOffersMessage{
		Offers: []*mesos.Offer{
			&mesos.Offer{
				Id:          &mesos.OfferID{Value: proto.String("offer-1")},
				FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
				SlaveId:     &mesos.SlaveID{Value: proto.String("test-slave-1")},
				Hostname:    proto.String("localhost"),
			},
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Unable to marshal ResourceOffersMessage, %v", err)
	}

	req := buildHttpRequest(t, "ResourceOffersMessage", data)
	resp := httptest.NewRecorder()

	// ServeHTTP will unmarshal msg and place on passed channel (above)
	proc.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}
}

func TestRescindOfferMessage(t *testing.T) {
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ {
			val, ok := msg.(*mesos.RescindResourceOfferMessage)
			if !ok {
				t.Fatal("Failed to receive msg of type RescindResourceOfferMessage")
			}
			if val.OfferId.GetValue() != "offer-2" {
				t.Fatal("Expected value not found in RescindResourceOfferMessage. See HTTP handler.")
			}
		}
	}()

	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}

	msg := &mesos.RescindResourceOfferMessage{
		OfferId: &mesos.OfferID{Value: proto.String("offer-2")},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Unable to marshal RescindResourceOfferMessage, %v", err)
	}

	req := buildHttpRequest(t, "RescindResourceOfferMessage", data)
	resp := httptest.NewRecorder()

	proc.ServeHTTP(resp, req)
	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}

}

func TestStatusUpdateMessage(t *testing.T) {
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ {
			val, ok := msg.(*mesos.StatusUpdateMessage)
			if !ok {
				t.Fatal("Failed to receive msg of type StatusUpdateMessage")
			}

			if val.Update.FrameworkId.GetValue() != "test-framework-1" {
				t.Fatal("Expected StatusUpdateMessage.FramewId not received.")
			}

			if val.Update.Status.GetState() != mesos.TaskState(mesos.TaskState_TASK_RUNNING) {
				t.Fatal("Expected StatusUpdateMessage.Update.Status.State not received.")
			}

			if string(val.Update.Status.GetData()) != "World!" {
				t.Fatal("Expected StatusUpdateMessage.Update.Message not received.")
			}

		}
	}()

	proc, err := newSchedulerProcess(eventQ)
	if err != nil {
		t.Fatal(err)
	}

	msg := &mesos.StatusUpdateMessage{
		Update: &mesos.StatusUpdate{
			FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
			Status: &mesos.TaskStatus{
				TaskId:  &mesos.TaskID{Value: proto.String("test-task-1")},
				State:   mesos.TaskState(mesos.TaskState_TASK_RUNNING).Enum(),
				Message: proto.String("Hello"),
				Data:    []byte("World!"),
			},
			Timestamp: proto.Float64(1234567.2),
			Uuid:      []byte("abcd-efg1-2345-6789-abcd-efg1"),
		},
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Unable to marshal StatusUpdateMessage, %v", err)
	}

	req := buildHttpRequest(t, "StatusUpdateMessage", data)
	resp := httptest.NewRecorder()

	proc.ServeHTTP(resp, req)
	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}
}

func TestSchedHttpProcStart(t *testing.T) {
	proc, err := newSchedulerProcess(make(chan interface{}))
	if err != nil {
		t.Fatal(err)
	}

	ctrlQ := make(chan int)
	go func() {
		proc.start()
		log.Println("Started Scheduler Process:", proc.server.Addr)
		ctrlQ <- 1
	}()
	<-ctrlQ

	// send test FrameworkRegistered messsage

	log.Println("Scheduler Process started, ID =", proc.processId.value)
	idreg := regexp.MustCompile(`^[a-z]+\(\d+\)@.*$`)
	if !idreg.MatchString(proc.processId.value) {
		t.Fatalf("ID value malformed. Got [%s]", proc.processId.value)
	}

}

func buildHttpRequest(t *testing.T, msgName string, data []byte) *http.Request {
	u, _ := address("127.0.0.1:5151").AsFullHttpURL(
		"/scheduler(1)/" + MESOS_INTERNAL_PREFIX + msgName)
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", "master(1)")
	return req
}
