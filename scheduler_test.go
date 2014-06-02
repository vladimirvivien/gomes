package gomes

import (
	"log"
	"os"
	"os/user"
	"testing"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	proto "code.google.com/p/goprotobuf/proto"
)

func TestScheDriverCreation(t *testing.T) {
	driver, err := NewSchedDriver(nil, &mesos.FrameworkInfo{}, "localhost:5050")
	if err != nil {
		t.Fatal ("Error creating SchedDriver with No Scheduler specified.", err)
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

func TestScheDriverCreation_WithFrameworkInfo_Override (t *testing.T) {
	driver, err := NewSchedDriver(
		nil, 
		&mesos.FrameworkInfo{
			User:proto.String("test-user"),
			Hostname:proto.String("test-host"),
		}, 
		"localhost:5050",
	)
	if err != nil {
		t.Fatal ("Error creating SchedulerDriver", err)
	}
	if driver.FrameworkInfo.GetUser() != "test-user" {
		t.Fatal("SchedulerDriver not setting User")
	}
	if driver.FrameworkInfo.GetHostname() != "test-host" {
		t.Fatal("SchedulerDriver not setting Hostname")
	}
}

func TestCreateNewSchedProc(t *testing.T) {
	driver := &SchedulerDriver {
		schedMsgQ : make(chan interface{}),
	}
	proc, err := createNewSchedProc(driver)
	if err != nil {
		t.Fatal("Error creating new SchedulerProcess:", err)
	}
	if proc.server.Addr == "" {
		t.Fatal("CreateNewSchedProc not setting default address")
	}
}


type mockScheduler string
func (sched mockScheduler) Registered(driver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo){
	log.Println ("mockScheduler.Registered() called...")
	log.Println ("MasterInfo.Id", masterInfo.GetId())
	if driver == nil {
		panic("Scheduler.Registered expects a SchedulerDriver, but got nil.")
	}
	if frameworkId == nil {
		panic("Scheduler.Registered expects frameworkId, but got nil.")
	}
	if frameworkId.GetValue() != "test-framework-1" {
		panic("Scheduler.Registered got unexpected frameworkId value: "+frameworkId.GetValue())
	}
	if masterInfo == nil {
		panic("Scheduler.Registered expects masterInfo, but got nil")
	}

	if masterInfo.GetId() != "localhost:5050" ||
	   masterInfo.GetIp() != 123456 ||
	   masterInfo.GetPort() != 12345 {
	   	panic("Scheduler.Registered expected MasterInfo values are missing.")
	}
}

func TestFrameworkRegisteredMessageHandling(t *testing.T) {
	
	driver := &SchedulerDriver {
		Scheduler : mockScheduler("Mock"),
		schedMsgQ : make(chan interface{}),
	}
	go setupSchedMsgQ(driver)

	msg := &mesos.FrameworkRegisteredMessage {
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:proto.String("localhost:5050"),
			Ip:proto.Uint32(123456),
			Port:proto.Uint32(12345),
		},
	}
	driver.schedMsgQ <- msg
}

