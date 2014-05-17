package gomes

import (
	_"log"
	_"fmt"
	_"net/url"
	_"net/http"
	_"net/http/httptest"
	_"io/ioutil"
	"regexp"
	"testing"
	_"code.google.com/p/goprotobuf/proto"
	//mesos "github.com/vladimirvivien/gomes/mesosproto"
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

func TestIdType(t *testing.T) {
	re := regexp.MustCompile(`^[a-z]+\(\d+\)$`)
	id := newID("scheduler")
	if !re.MatchString(string(id)) {
		t.Error("Type ID not generating proper ID value")
	}
}

// func TestMasterClient_RegisterFramework(t *testing.T) {
// 	schedulerId := PID("scheduler(1)@127.0.0.1:8080")
// 	// Server-side Validation
// 	server := makeMockServer(func (rsp http.ResponseWriter, req *http.Request){
// 		if req.Header.Get("Connection") != "Keep-Alive" {
// 			t.Fatalf("Expected Connection Header not found")
// 		}
		
// 		if req.URL.Path != "/master" + REG_FRAMEWORK_CMD {
// 			t.Fatalf("Expected URL path not found.")
// 		}

// 		expectedUa := string(schedulerId)
// 		ua := req.Header.Get("Libprocess-From")
// 		if ua != expectedUa {
// 			t.Fatalf("User-Agent value malformed expecting %s but got %s", expectedUa, ua)
// 		}

// 		data, err := ioutil.ReadAll(req.Body)
// 		if err != nil{
// 			t.Fatalf("Unable to get FrameworkInfo data")
// 		}
// 		defer req.Body.Close()

// 		regMsg := new (mesos.RegisterFrameworkMessage)
// 		err = proto.Unmarshal(data, regMsg)
// 		if err != nil {
// 			t.Fatal("Problem unmarshaling expected RegisterFrameworkMessage")
// 		}
// 		info := regMsg.Framework
// 		if *info.User != "test-user" ||
// 		   *info.Name != "test-name" ||
// 		   *info.Id.Value != "test-framework-1" {
// 		   t.Fatalf("Got bad FrameworkInfo values %s, %s, %s", info.User, info.Name, info.Id.Value )
// 		}

// 		rsp.WriteHeader(http.StatusAccepted)
// 		fmt.Print (rsp)

// 	})
// 	defer server.Close()

// 	url, _ := url.Parse(server.URL)
// 	// Test Data
// 	master := NewMasterClient(PID("master@"+url.Host))

// 	regMsg := &mesos.RegisterFrameworkMessage{
// 	    Framework: &mesos.FrameworkInfo {
// 			User:proto.String("test-user"),
// 			Name:proto.String("test-name"),
// 			Id:&mesos.FrameworkID{Value: proto.String("test-framework-1")},
// 		},
// 	}

// 	master.RegisterFramework(schedulerId,*regMsg)
// }

// func makeMockServer(handler func (rsp http.ResponseWriter, req *http.Request)) *httptest.Server{
// 	server := httptest.NewServer(http.HandlerFunc(handler))
// 	log.Println("Created server  " + server.URL)
// 	return server
// }