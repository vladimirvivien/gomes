package gomes

import (
	"log"
	"fmt"
	"net/url"
	"net/http"
	"net/http/httptest"
	"io/ioutil"
	"regexp"
	"testing"
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func TestAddressType(t *testing.T) {
	addr := address("127.0.0.1:5050")
	if (addr != "127.0.0.1:5050"){
		t.Error("Address type value not translated to string")
	}

	u, err := addr.AsURL()
	if(err != nil ){
		t.Error("address.AsURL() failed:", err)
	}
	if (u.Host != "127.0.0.1:5050"){
		t.Error("Address.AsURL() host not converted")
	}
	if(u.Scheme != HTTP_SCHEME){
		t.Error("Address.AsURL() Scheme not converted: ", u.Scheme)
	}
}

func TestMasterClient_RegisterFramework(t *testing.T) {
	idreg := regexp.MustCompile(`^[a-z]+\(\d+\).*$`)

	// Server-side Validation
	server := makeMockServer(func (rsp http.ResponseWriter, req *http.Request){
		if req.Header.Get("Connection") != "Keep-Alive" {
			t.Fatalf("Expected Connection Header not found")
		}
		
		cmdPath := buildReqPath(MESSAGE_REG_FRAMEWORK)
		if req.URL.Path != cmdPath {
			t.Fatalf("Expected URL path not found.")
		}

		proc := req.Header.Get("Libprocess-From")
		if  !idreg.MatchString(proc) {
			t.Fatalf("Libprocess-From value malformed. Got [%s]", proc)
		}

		data, err := ioutil.ReadAll(req.Body)
		if err != nil{
			t.Fatalf("Unable to get FrameworkInfo data")
		}
		defer req.Body.Close()

		regMsg := new (mesos.RegisterFrameworkMessage)
		err = proto.Unmarshal(data, regMsg)
		if err != nil {
			t.Fatal("Problem unmarshaling expected RegisterFrameworkMessage")
		}
		info := regMsg.Framework
		if info.GetUser() != "test-user" ||
		   info.GetName() != "test-name" ||
		   info.Id.GetValue() != "test-framework-1" {
		   t.Fatalf("Got bad FrameworkInfo values %s, %s, %s", info.User, info.Name, info.Id.Value )
		}

		rsp.WriteHeader(http.StatusAccepted)
		fmt.Print (rsp)

	})
	defer server.Close()

	url, _ := url.Parse(server.URL)
	// Test Data
	master := newMasterClient(url.Host)

	framework := &mesos.FrameworkInfo {
		User:proto.String("test-user"),
		Name:proto.String("test-name"),
		Id:&mesos.FrameworkID{Value: proto.String("test-framework-1")},
	}

	master.RegisterFramework(newID("scheduler"),framework)
}

func makeMockServer(handler func (rsp http.ResponseWriter, req *http.Request)) *httptest.Server{
	server := httptest.NewServer(http.HandlerFunc(handler))
	log.Println("Created server  " + server.URL)
	return server
}