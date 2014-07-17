package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"testing"
	"time"
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
		nil, NewFrameworkInfo(
			"test-user",
			"test-name",
			NewFrameworkID("test-framework"),
		),
		"localhost:5050",
	)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}
	if driver.FrameworkInfo.GetUser() != "test-user" {
		t.Fatal("SchedulerDriver not setting User")
	}
	if driver.Master != "localhost:5050" {
		t.Fatal("SchedulerDriver not setting Master")
	}
}

func TestDriverStart(t *testing.T) {
	// test server to accept Framework Registration
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()
	url, _ := url.Parse(server.URL)
	driver, err := NewSchedDriver(
		nil,
		NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		url.Host,
	)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}
	stat := driver.Start()
	if stat == mesos.Status_DRIVER_RUNNING {
		// simulate registered event
		msg := &mesos.FrameworkRegisteredMessage{
			FrameworkId: NewFrameworkID("framework-1"),
			MasterInfo:  NewMasterInfo("master-1", 12345, 1234),
		}
		driver.schedMsgQ <- msg
		time.Sleep(time.Millisecond * 21)
	} else {
		t.Fatal("SchedulerDriver.Start() - failed to start:", stat, ". Expecting DRIVER_RUNNING ")
	}

	if !driver.connected {
		t.Fatal("SchedulerDriver.Start() not setting connected flag.")
	}

	if driver.failover {
		t.Fatal("SchedulerDriver.Start() not setting failover flag.")
	}
}

func TestDriverStart_WithNoMasterAvailable(t *testing.T) {
	driver, err := NewSchedDriver(
		nil,
		NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		"localhost:50501")
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	stat := driver.Start()
	if stat == mesos.Status_DRIVER_RUNNING {
		t.Fatal("SchedulerDriver.start() - should failed due to connection refused, but got:", stat)
	}
}

func TestDriverJoin(t *testing.T) {
	driver, err := NewSchedDriver(
		nil,
		NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		":15050",
	)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	driver.Status = mesos.Status_DRIVER_RUNNING
	go func() {
		stat := driver.Join()
		if stat != mesos.Status_DRIVER_STOPPED {
			t.Fatal("Expected mesos.Status_DRIVER_STOPPED, but got", stat)
			<-driver.controlQ // bleed chan
		}

	}()
	driver.controlQ <- mesos.Status_DRIVER_STOPPED
}

func TestDriverRun(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()
	url, _ := url.Parse(server.URL)
	driver, err := NewSchedDriver(
		nil, NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		url.Host)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	go func() {
		stat := driver.Run()
		if stat != mesos.Status_DRIVER_ABORTED {
			t.Fatal("Expected mesos.Status_DRIVER_ABORTED, but got ", stat)
			<-driver.controlQ // bleed chan
		}
	}()

	driver.controlQ <- mesos.Status_DRIVER_ABORTED
}

func TestDriverStop(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()
	url, _ := url.Parse(server.URL)
	driver, err := NewSchedDriver(
		nil,
		NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		url.Host)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	go func() {
		stat := driver.Run()
		log.Println("Stopping driver....")
		if stat != mesos.Status_DRIVER_STOPPED {
			t.Fatal("Expected mesos.Status_DRIVER_STOPPED, but got ", stat)
			<-driver.controlQ // bleed chan
		}
	}()
	time.Sleep(101 * time.Millisecond) // stall.
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		t.Fatal("Expected DRIVER_RUNNING, but got ", driver.Status)
	}
	driver.Stop(false)
}

func TestDriverAbort(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()
	url, _ := url.Parse(server.URL)
	driver, err := NewSchedDriver(nil,
		NewFrameworkInfo("test", "test-framework-1", NewFrameworkID("test-id")),
		url.Host)
	if err != nil {
		t.Fatal("Error creating SchedulerDriver", err)
	}

	go func() {
		stat := driver.Run()
		if stat != mesos.Status_DRIVER_ABORTED {
			t.Fatal("Expected mesos.Status_DRIVER_STOPPED, but got ", stat)
			<-driver.controlQ // bleed chan
		}
	}()
	time.Sleep(101 * time.Millisecond) // stall.
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		t.Fatal("Expected DRIVER_RUNNING, but got ", driver.Status)
	}
	driver.Abort()
}

func TestFrameworkRegisteredMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.Registered = func(driver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
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

	msg := &mesos.FrameworkRegisteredMessage{
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		MasterInfo: &mesos.MasterInfo{
			Id:   proto.String("localhost:0"),
			Ip:   proto.Uint32(123456),
			Port: proto.Uint32(12345),
		},
	}
	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestFrameworkReRegisteredMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.Reregistered = func(driver *SchedulerDriver, masterInfo *mesos.MasterInfo) {
		if masterInfo == nil {
			log.Fatalf("Scheduler.Reregistered expects masterInfo, but got nil")
		}

		if masterInfo.GetId() != "master-1" ||
			masterInfo.GetIp() != 123456 ||
			masterInfo.GetPort() != 12345 {
			log.Fatalf("Scheduler.Registered expected MasterInfo values are missing.")
		}
	}

	msg := &mesos.FrameworkReregisteredMessage{
		FrameworkId: NewFrameworkID("test-framework"),
		MasterInfo:  NewMasterInfo("master-1", 12345, 123456),
	}
	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestResourceOffersMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.ResourceOffers = func(driver *SchedulerDriver, offers []*mesos.Offer) {
		if len(offers) != 1 {
			log.Fatalf("Scheduler.ResourceOffers expected 1 offer, but got", len(offers))
		}
		if offers[0].GetId().GetValue() != "offer-1" {
			log.Fatalln("Scheduler.ResourceOffers exepected value not received")
		}
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
	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestRescindOfferMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.OfferRescinded = func(driver *SchedulerDriver, offerId *mesos.OfferID) {
		if offerId.GetValue() != "offer-2" {
			log.Fatalln("Scheduler.OfferRescinded exepected value not received")
		}
	}

	msg := &mesos.RescindResourceOfferMessage{
		OfferId: &mesos.OfferID{Value: proto.String("offer-2")},
	}

	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestStatusUpdateMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.StatusUpdate = func(schedDriver *SchedulerDriver, taskStatus *mesos.TaskStatus) {
		if taskStatus.GetState() != mesos.TaskState(mesos.TaskState_TASK_RUNNING) {
			log.Fatal("Scheduler.StatusUpdate expected State value not received.")
		}

		if string(taskStatus.GetData()) != "World!" {
			log.Fatal("Scheduler.StatusUpdate expected Status.Data not received.")
		}
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

	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestFrameworkMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.FrameworkMessage = func(schedDriver *SchedulerDriver, execId *mesos.ExecutorID, slaveId *mesos.SlaveID, data []byte) {
		if execId.GetValue() != "test-executor-1" {
			log.Fatal("Scheduler.FrameworkMessage.ExecutorId not received.")
		}

		if slaveId.GetValue() != "test-slave-1" {
			log.Fatal("Scheduler.FrameworkMessage.SlaveId not received.")
		}

		if string(data) != "Hello-Test" {
			log.Fatal("Scheduler.FrameworkMessage.Data not received.")
		}
	}

	msg := &mesos.ExecutorToFrameworkMessage{
		SlaveId:     &mesos.SlaveID{Value: proto.String("test-slave-1")},
		FrameworkId: &mesos.FrameworkID{Value: proto.String("test-framework-1")},
		ExecutorId:  &mesos.ExecutorID{Value: proto.String("test-executor-1")},
		Data:        []byte("Hello-Test"),
	}
	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg

}

func TestSlaveLostMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.SlaveLost = func(driver *SchedulerDriver, slaveId *mesos.SlaveID) {
		if slaveId.GetValue() != "test-slave-1" {
			log.Fatal("Scheduler.SlaveLost.SlaveID not received.")
		}
	}

	msg := &mesos.LostSlaveMessage{SlaveId: &mesos.SlaveID{Value: proto.String("test-slave-1")}}

	driver, err := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	driver.schedMsgQ <- msg
}

func TestErrorMessageHandling(t *testing.T) {
	sched := NewMesosScheduler()
	sched.Error = func(driver *SchedulerDriver, err MesosError) {
		if &err == nil {
			t.Fatal("SchedulerDriver expected Error to be generated, but got nil.")
		}

		// make sure driver aborted.
		if driver.Status != mesos.Status_DRIVER_ABORTED {
			t.Fatalf("Expected SchedulerDriver to have status, %s, but is %s", mesos.Status_DRIVER_ABORTED, driver.Status)
		}
	}

	driver, _ := NewSchedDriver(sched, &mesos.FrameworkInfo{}, "localhost:0")
	driver.schedMsgQ <- "Hello"
}
