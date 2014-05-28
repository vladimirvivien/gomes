package gomes

import (
    mesos "github.com/vladimirvivien/gomes/mesosproto"
    _"code.google.com/p/goprotobuf/proto"
)


type Scheduler interface {
	Registered(schedulerDriver *SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) (error)
}

type SchedulerDriver struct {
	Master string
	Scheduler *Scheduler
	FrameworkInfo *mesos.FrameworkInfo
	schedProc schedulerProcess
}

func NewSchedDriver(scheduler *Scheduler, framework *mesos.FrameworkInfo, master string) *SchedulerDriver {
	// things to do here:
	// ensure framework.user is set
	// if scheduler is null, abort
	// ensure master string is ok
	// if all works, returns a shiny new SchedDriver.
	return &SchedulerDriver{Master:master, Scheduler: scheduler}
}

// func NewAuthSchedDriver() // reserved for authenticated Driver.
// func (driver *SchedulerDriver) Start() (error) {}