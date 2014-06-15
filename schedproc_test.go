package gomes

import (
	"log"
	"testing"
	"regexp"
	"bytes"
	"net/http"
	"net/http/httptest"
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
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
	if !re3.MatchString (id3.prefix){
		t.Error("SchedID has invalid prefix:", id3.prefix)
	}
}

func TestNewFullSchedID(t *testing.T) {
	re1 :=regexp.MustCompile(`scheduler\(\d\)@machine1:4040`)
	id1 := newSchedProcID("machine1:4040")
	if !re1.MatchString(id1.value){
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

func TestFrameworkRegisteredMessage(t *testing.T) {
	// setup chanel to receive unmarshalled message
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ{
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
		t.Fatal (err)
	}
	msg := &mesos.FrameworkRegisteredMessage {
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:proto.String("master-1"),
			Ip:proto.Uint32(123456),
			Port:proto.Uint32(12345),
		},
	}
	data, err := proto.Marshal(msg)
	if (err != nil){
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

func TestSchedHttpProcStart(t *testing.T) {
	proc, err := newSchedulerProcess(make(chan interface{}))
	if err != nil {
		t.Fatal(err)
	}
	
	ctrlQ := make(chan int)
	go func() {
		proc.start()
		log.Println ("Started Scheduler Process:", proc.server.Addr)
		ctrlQ <- 1
	}()
	<- ctrlQ

	// send test FrameworkRegistered messsage


	log.Println("Scheduler Process started, ID =", proc.processId.value)
	idreg := regexp.MustCompile(`^[a-z]+\(\d+\)@.*$`)
	if  !idreg.MatchString(proc.processId.value) {
			t.Fatalf("ID value malformed. Got [%s]", proc.processId.value)
	}

}

func buildHttpRequest(t *testing.T, msgName string, data []byte) *http.Request{
	u, _ := address("127.0.0.1:5151").AsFullHttpURL(
		"/scheduler(1)/"+MESOS_INTERNAL_PREFIX+msgName)
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", "master(1)")
    return req
}