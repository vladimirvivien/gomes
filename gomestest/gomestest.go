package main

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/vladimirvivien/gomes"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"log"
)

var Sched *gomes.Scheduler = gomes.NewMesosScheduler()

func init() {
	log.Println("Initializing the Scheduler...")
	Sched.Registered = func(driver *gomes.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
		log.Println("Framework Registered with Master ", masterInfo)
	}

	Sched.Reregistered = func(driver *gomes.SchedulerDriver, masterInfo *mesos.MasterInfo) {
		log.Println("Framework Registered with Master ", masterInfo)
	}

	Sched.ResourceOffers = func(driver *gomes.SchedulerDriver, offers []*mesos.Offer) {
		log.Println("Got ", len(offers), "from master.")
		log.Println("Offer 1", offers[0])
	}

	Sched.Error = func(driver *gomes.SchedulerDriver, err gomes.MesosError) {
		log.Println("Scheduler received error:", err.Error())
	}
}

func main() {
	log.Println("Starting gomestest.")
	log.Println("Assuming master 127.0.0.1:5050...")
	master := "127.0.0.1:5050"

	framework := &mesos.FrameworkInfo{
		User: proto.String("test"),
		Name: proto.String("gomes"),
		Id:   &mesos.FrameworkID{Value: proto.String("gomes-framework-1")},
	}

	log.Println("Registering framework" + framework.String())

	driver, err := gomes.NewSchedDriver(Sched, framework, master)
	if err != nil {
		log.Println("Unable to create a SchedulerDriver", err.Error())
	}

	stat := driver.Run()
	if stat != mesos.Status_DRIVER_STOPPED {
		log.Println("Framework did not stop properly.")
	}
}
