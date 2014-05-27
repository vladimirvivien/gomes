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

func TestSchedProcCreation(t *testing.T) {
	proc := newSchedHttpProcess(4000)
	if proc.server == nil {
		t.Error("SchedHttpProcess missing server")
	}
	if proc.server.Addr != ":4000" {
		t.Error("SchedHttpProcess not setting address properly")
	}
	idreg := regexp.MustCompile(`^[a-z]+\(\d+\).*$`)
	if  !idreg.MatchString(proc.processId) {
			t.Fatalf("ID value malformed. Got [%s]", proc)
	}
}

func TestSchedHttpHandler(t *testing.T) {
	handler := schedHttpHandler
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

	// build req from master
	u, _ := address("127.0.0.1:5050").AsURL()
	u.Path="/scheduler(1)/FrameworkRegisteredMessage"
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", "master(1)")

	resp := httptest.NewRecorder()
	handler(resp, req)
	if resp.Code != http.StatusAccepted {
		t.Fatalf("Expecting server status %d but got status %d", http.StatusAccepted, resp.Code)
	}
}

func TestSchedHttpProcStart(t *testing.T) {
	//proc := newSchedHttpProcess(4000)
}