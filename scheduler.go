package gomes

import (
	"fmt"
	"os"
	"os/user"
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    proto "code.google.com/p/goprotobuf/proto"
)

type Scheduler interface {
	Registered(schedulerDriver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo)
}

type SchedulerDriver struct {
	Master string
	Scheduler Scheduler
	FrameworkInfo *mesos.FrameworkInfo

	masterClient *masterClient
	schedMsgQ chan interface{}
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
	}

	proc, err := createNewSchedProc(driver)
	if (err != nil){
		return nil, err
	}
	driver.schedProc = proc
	
	go setupSchedMsgQ(driver)

	driver.masterClient = newMasterClient(master)

	return driver, nil
}

func (driver *SchedulerDriver) Start() (error) {
	driver.masterClient.RegisterFramework(driver.schedProc.processId, driver.FrameworkInfo)
	// spin up eventQ processor
	return nil
}

func createNewSchedProc(driver *SchedulerDriver) (*schedulerProcess, error) {
	localAddress, err := os.Hostname()
	if err != nil {
		localAddress = "localhost"
	}

	return newSchedulerProcess(localAddress, driver.schedMsgQ)
}

func setupSchedMsgQ(driver *SchedulerDriver){
	sched := driver.Scheduler
	for event := range driver.schedMsgQ {
		switch event.(type) {
			case *mesos.FrameworkRegisteredMessage:
				msg, ok := event.(*mesos.FrameworkRegisteredMessage)
				if ok && sched != nil {
					sched.Registered(driver, msg.FrameworkId, msg.MasterInfo)
				}
			default:
		}
	}
}
