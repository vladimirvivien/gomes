package gomes

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"testing"

	proto "code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func TestScheDriverCreation(t *testing.T) {
	driver, err := NewSchedDriver(nil, &mesos.FrameworkInfo{}, "localhost:5050")
	if err != nil {
		t.Fatal("Error creating SchedDriver with No Scheduler specified.", err)
	}

	if driver.Master != "localhost:5050" {
		t.Fatal("Error creating SchedDriver, Master not set.")
	}

	if driver.FrameworkInfo == nil {
		t.Fatal("Error creating SchedDriver, FrameworkInfo not set.")
	}

	user, _ := user.Current()
	if driver.FrameworkInfo.GetUser() != user.Username {
		t.Fatal("SchedDriver not setting default Username")
	}

	host, _ := os.Hostname()
	if driver.FrameworkInfo.GetHostname() != host {
		t.Fatal("SchedDriver not setting default Host")
	}
}

func TestScheDriverCreation_WithFrameworkInfo_Override(t *testing.T) {
	driver, err := NewSchedDriver(
		nil,
		&mesos.FrameworkInfo{
			User:     proto.String("test-user"),
			Hostname: proto.String("test-host"),
		},
		"localhost:5050",
	)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}
	if driver.FrameworkInfo.GetUser() != "test-user" {
		t.Fatal("SchedulerDriver not setting User")
	}
	if driver.FrameworkInfo.GetHostname() != "test-host" {
		t.Fatal("SchedulerDriver not setting Hostname")
	}
}

func TestStartDriver(t *testing.T) {
	// test server to accept Framework Registration
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()
	url, _ := url.Parse(server.URL)
	driver, err := NewSchedDriver(nil, makeMockFrameworkInfo(), url.Host)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}
	stat := driver.Start()
	if stat != mesos.Status_DRIVER_RUNNING {
		t.Fatal("SchedulerDriver.start() - failed to start:", stat, ". Expecting DRIVER_RUNNING ")
	}
}

func TestStartDriver_WithNoMasterAvailable(t *testing.T) {
	driver, err := NewSchedDriver(nil, makeMockFrameworkInfo(), "localhost:5050")
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	stat := driver.Start()
	if stat == mesos.Status_DRIVER_RUNNING {
		t.Fatal("SchedulerDriver.start() - should failed due to connection refused, but got:", stat)
	}
}

func TestJoinDriver(t *testing.T) {

}

type mockScheduler string

func (sched mockScheduler) Registered(driver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Println("mockScheduler.Registered() called...")
	if driver == nil {
		log.Fatalf("Scheduler.Registered expects a SchedulerDriver, but got nil.")
	}
	if frameworkId == nil {
		log.Fatalf("Scheduler.Registered expects frameworkId, but got nil.")
	}
	if frameworkId.GetValue() != "test-framework-1" {
		log.Fatalf("Scheduler.Registered got unexpected frameworkId value: " + frameworkId.GetValue())
	}
	if masterInfo == nil {
		log.Fatalf("Scheduler.Registered expects masterInfo, but got nil")
	}

	if masterInfo.GetId() != "localhost:0" ||
		masterInfo.GetIp() != 123456 ||
		masterInfo.GetPort() != 12345 {
		log.Fatalf("Scheduler.Registered expected MasterInfo values are missing.")
	}
}

func (sched mockScheduler) Reregistered(driver *SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Println("mockScheduler.Reregistered() called...")
	if driver == nil {
		log.Fatalf("Scheduler.Reregistered expects a SchedulerDriver, but got nil.")
	}
	if masterInfo == nil {
		log.Fatalf("Scheduler.Reregistered expects masterInfo, but got nil")
	}

	if masterInfo.GetId() != "master-1" ||
		masterInfo.GetIp() != 123456 ||
		masterInfo.GetPort() != 12345 {
		log.Fatalf("Scheduler.Registered expected MasterInfo values are missing.")
	}
}

func (sched mockScheduler) ResourceOffers(driver *SchedulerDriver, offers []*mesos.Offer) {
	log.Println("mockScheduler.ResourceOffers called...")
	if len(offers) != 1 {
		log.Fatalf("Scheduler.ResourceOffers expected 1 offer, but got", len(offers))
	}
	if offers[0].GetId().GetValue() != "offer-1" {
		log.Fatalln("Scheduler.ResourceOffers exepected value not received")
	}
}

func (sched mockScheduler) Error(driver *SchedulerDriver, err MesosError) {
	log.Println("mockScheduler.Error() called...")
	log.Println("SchedulerDriver received an error: " + err.Error())
}

func TestFrameworkRegisteredMessageHandling(t *testing.T) {
	msg := &mesos.FrameworkRegisteredMessage{
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:   proto.String("localhost:0"),
			Ip:   proto.Uint32(123456),
			Port: proto.Uint32(12345),
		},
	}
	driver, err := NewSchedDriver(mockScheduler("Mock"), &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestFrameworkReRegisteredMessageHandling(t *testing.T) {
	msg := &mesos.FrameworkReregisteredMessage{
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:   proto.String("master-1"),
			Ip:   proto.Uint32(123456),
			Port: proto.Uint32(12345),
		},
	}
	driver, err := NewSchedDriver(mockScheduler("Mock"), &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestResourceOffersMessageHandling(t *testing.T) {
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
	driver, err := NewSchedDriver(mockScheduler("Mock"), &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestErrorMessageHandling(t *testing.T) {
	driver, _ := NewSchedDriver(mockScheduler("Mock"), &mesos.FrameworkInfo{}, "localhost:0")
	driver.schedMsgQ <- "Hello"
}
