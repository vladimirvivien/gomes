package gomes

import (
	"fmt"
	"os"
	"os/user"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    proto "code.google.com/p/goprotobuf/proto"
)

type MesosError string
func NewMesosError(msg string) MesosError{
	return MesosError(msg)
}
func (err MesosError) Error() string {
	return string(err)
}

type Scheduler interface {
	Registered(schedDriver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo)
	Reregistered(schedDriver *SchedulerDriver, masterInfo *mesos.MasterInfo)
	ResourceOffers(schedDriver *SchedulerDriver, offers []*mesos.Offer)
	Error(schedDriver *SchedulerDriver, err MesosError)
}


type SchedulerDriver struct {
	Master string
	Scheduler Scheduler
	FrameworkInfo *mesos.FrameworkInfo

	status mesos.Status
	masterClient *masterClient
	schedMsgQ chan interface{}
	controlQ chan mesos.Status
	schedProc *schedulerProcess
}

func NewSchedDriver(scheduler Scheduler, framework *mesos.FrameworkInfo, master string) (*SchedulerDriver, error) {
	if master == "" {
		return nil, fmt.Errorf("Missing master address.")
	}

	if framework == nil {
		return nil, fmt.Errorf("Missing FrameworkInfo.")
	}

	// set default userid
	if framework.GetUser() == ""{
		user, err := user.Current()
		if err != nil || user == nil {
			framework.User = proto.String("unknown")
		}else{
			framework.User = proto.String(user.Username)
		}
	}

	// set default hostname
	if framework.GetHostname() == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		framework.Hostname = proto.String(host)
	}

	driver := &SchedulerDriver{
		Master: master, 
		Scheduler: scheduler, 
		FrameworkInfo:framework,
		schedMsgQ: make(chan interface{}, 10), 
		controlQ : make(chan mesos.Status),
	}

	proc,err := newSchedulerProcess(driver.schedMsgQ)
	if err != nil {
		return nil, err
	}
	
	driver.schedProc = proc

	go setupSchedMsgQ(driver)

	driver.masterClient = newMasterClient(master)

	driver.status = mesos.Status_DRIVER_NOT_STARTED

	return driver, nil
}

func (driver *SchedulerDriver) Start() mesos.Status {
	if driver.status != mesos.Status_DRIVER_NOT_STARTED {
		return driver.status
	}
	driver.schedProc.start() 
	
	// TODO: should ping scheduler process here to make sure 
	// http process is up and running with no issue.
	err := driver.masterClient.RegisterFramework(driver.schedProc.processId, driver.FrameworkInfo)
	if err != nil {
		driver.status = mesos.Status_DRIVER_NOT_STARTED
		if driver.Scheduler != nil {
			driver.Scheduler.Error(driver, MesosError("Failed to register the framework:"+err.Error()))
		}
	}else{
		driver.status = mesos.Status_DRIVER_RUNNING
	}
	return driver.status
}

func (driver *SchedulerDriver) Join() mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING{
		return driver.status
	}

	return <-driver.controlQ
}

func (driver *SchedulerDriver) Run() mesos.Status {
	go func(){
		stat := driver.Start()
		driver.controlQ <- stat
	}()
	stat := <- driver.controlQ

	if stat != mesos.Status_DRIVER_RUNNING{
		return stat
	}
	return driver.Join()
}

func setupSchedMsgQ(driver *SchedulerDriver){
	sched := driver.Scheduler
	for event := range driver.schedMsgQ {
		switch event.(type) {
			case *mesos.FrameworkRegisteredMessage:
				if msg, ok := event.(*mesos.FrameworkRegisteredMessage); ok {
					go sched.Registered(driver, msg.FrameworkId, msg.MasterInfo)
				}else {
					go sched.Error(driver, "Failed to cast received Protobuf.Message to mesos.FrameworkRegisteredMessage")
				}
			case *mesos.FrameworkReregisteredMessage:
				if msg, ok := event.(*mesos.FrameworkReregisteredMessage); ok {
					go sched.Reregistered(driver, msg.MasterInfo)
				}else {
					go sched.Error(driver, "Failed to cast received Protobuf.Message to mesos.FrameworkReregisteredMessage")
				}
			case *mesos.ResourceOffersMessage:
				if msg, ok := event.(*mesos.ResourceOffersMessage); ok {
					go sched.ResourceOffers(driver, msg.Offers)
				}else {
					go sched.Error(driver, "Failed to cast received Protobuf.Message to mesos.FrameworkRegisteredMessage")
				}								
			default:
				sched.Error(driver, "Received unexpected event from server.")
		}
	}
}
