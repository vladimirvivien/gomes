package gomes

import (
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

type Scheduler struct {
	Registered       func(*SchedulerDriver, *mesos.FrameworkID, *mesos.MasterInfo)
	Reregistered     func(*SchedulerDriver, *mesos.MasterInfo)
	ResourceOffers   func(*SchedulerDriver, []*mesos.Offer)
	OfferRescinded   func(*SchedulerDriver, *mesos.OfferID)
	StatusUpdate     func(*SchedulerDriver, *mesos.TaskStatus)
	FrameworkMessage func(*SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, []byte)
	SlaveLost        func(*SchedulerDriver, *mesos.SlaveID)
	Error            func(*SchedulerDriver, MesosError)
}

func NewMesosScheduler() *Scheduler {
	return &Scheduler{}
}
