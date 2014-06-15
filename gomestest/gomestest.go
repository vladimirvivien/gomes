package main

import (
	"log"
	"fmt"
	"code.google.com/p/goprotobuf/proto"
	"github.com/vladimirvivien/gomes"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func main() {
	master := "127.0.0.1:5050"

	framework := &mesos.FrameworkInfo {
			User: proto.String("test"),
			Name: proto.String("gomes"),
			Id:&mesos.FrameworkID{Value: proto.String("gomes-framework-1")},
	}

	driver, err := gomes.NewSchedDriver(mesosScheduler{}, framework ,master)
	if err != nil{
		log.Println("Unable to create a SchedulerDriver", err.Error())
	}

	ch := make(chan bool)
	go func(){
		stat := driver.Start()
		if stat != mesos.Status_DRIVER_RUNNING {
			fmt.Println ("Encountered error, scheduler drive not running.")
		}else{
			fmt.Println ("Framework Running!")
		}
	}()
	<-ch
}

type mesosScheduler struct {}
func (sched mesosScheduler) Registered(driver *gomes.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo){
	log.Println ("Framework Registered with Master ", masterInfo)
}
func(sched mesosScheduler) Error(driver *gomes.SchedulerDriver, err gomes.MesosError){
	log.Println("Scheduler received error:", err.Error())
}