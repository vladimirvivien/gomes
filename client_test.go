package ionos

import (
	"log"
	"net/url"
	"net/http"
	"net/http/httptest"
	"io/ioutil"
	"testing"
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/ionos/mesosproto"
)

func TestPIDType(t *testing.T) {
	pid := PID("master@127.0.0.1:5050")
	if (pid != "master@127.0.0.1:5050"){
		t.Error("PID string value not translated to URL")
	}

	u, err := pid.AsURL()
	if(err != nil ){
		t.Error("PID.AsURL() failed:", err)
	}
	if (u.Host != "127.0.0.1:5050"){
		t.Error("PID.AsURL() host not converted")
	}
	if(u.Scheme != HTTP_SCHEME){
		t.Error("PID.AsURL() Scheme not converted: ", u.Scheme)
	}
}

func TestMasterClient_RegisterFramework(t *testing.T) {
	schedulerId := PID("scheduler(1)@127.0.0.1:8080")
	// Server-side Validation
	server := makeMockServer(func (rsp http.ResponseWriter, req *http.Request){
		if req.Header.Get("Connection") != "Keep-Alive" {
			t.Fatalf("Expected Connection Header not found")
		}
		
		if req.URL.Path != REG_FRAMEWORK_CMD {
			t.Fatalf("Expected URL path not found.")
		}

		expectedUa := USER_AGENT_PREFIX + string(schedulerId)
		ua := req.Header.Get("User-Agent")
		if ua != expectedUa {
			t.Fatalf("User-Agent value malformed expecting %s but got %s", expectedUa, ua)
		}

		data, err := ioutil.ReadAll(req.Body)
		if err != nil{
			t.Fatalf("Unable to get FrameworkInfo data")
		}
		defer req.Body.Close()

		info := new (mesos.FrameworkInfo)
		err = proto.Unmarshal(data, info)
		if err != nil {
			t.Fatal("Problem unmarshaling expected FrameworkInfo")
		}
		if *info.User != "test-user" ||
		   *info.Name != "test-name" ||
		   *info.Id.Value != "test-framework-1" {
		   t.Fatalf("Got bad FrameworkInfo values %s, %s, %s", info.User, info.Name, info.Id.Value )
		}

	})
	defer server.Close()

	url, _ := url.Parse(server.URL)
	// Test Data
	master := NewMasterClient(PID("master@"+url.Host))

	info := &mesos.FrameworkInfo {
		User: proto.String("test-user"),
		Name: proto.String("test-name"),
		Id:&mesos.FrameworkID{Value: proto.String("test-framework-1")},
	}

	master.RegisterFramework(schedulerId,*info)
}

func makeMockServer(handler func (rsp http.ResponseWriter, req *http.Request)) *httptest.Server{
	server := httptest.NewServer(http.HandlerFunc(handler))
	log.Println("Created server  " + server.URL)
	return server
}