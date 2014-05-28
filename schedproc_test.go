package gomes

import (
	"testing"
	"regexp"
	"bytes"
	"net/http"
	"net/http/httptest"
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func TestIdType(t *testing.T) {
	re := regexp.MustCompile(`^[a-z]+\(\d+\)$`)
	id := newID("scheduler")
	if !re.MatchString(string(id)) {
		t.Error("Type ID not generating proper ID value")
	}
}

// func TestSchedProcCreation(t *testing.T) {
// 	proc := newSchedulerProcess(":4000", make(chan interface{}))
// 	if proc.server == nil {
// 		t.Error("SchedHttpProcess missing server")
// 	}
// 	if proc.server.Addr != ":4000" {
// 		t.Error("SchedHttpProcess not setting address properly")
// 	}
// 	idreg := regexp.MustCompile(`^[a-z]+\(\d+\).*$`)
// 	if  !idreg.MatchString(proc.processId) {
// 			t.Fatalf("ID value malformed. Got [%s]", proc)
// 	}
// }

func TestFrameworkRegisteredMessage(t *testing.T) {
	// setup chanel to receive unmarshalled message
	eventQ := make(chan interface{})
	go func() {
		for msg := range eventQ{
			if _, ok := msg.(mesos.FrameworkRegisteredMessage); !ok {
				t.Fatal("Failed to receive msg of type FrameworkRegisteredMessage")
			}
		}
	}()

	// Simulate FramworkRegisteredMessage request from master.
	proc := newSchedulerProcess(":0606", eventQ)
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
	//proc := newSchedHttpProcess(4000)
}

func buildHttpRequest(t *testing.T, msgName string, data []byte) *http.Request{
	u, _ := address("127.0.0.1:5151").AsURL()
	u.Path="/scheduler(1)/"+msgName
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", "master(1)")
    return req
}