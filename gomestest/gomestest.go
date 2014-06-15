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

	driver, err := gomes.NewSchedDriver(nil, framework ,master)
	if err != nil{
		log.Println("Unable to create a SchedulerDriver", err.Error())
	}

	stat := driver.Start()

	if stat != mesos.Status_DRIVER_RUNNING {
		fmt.Println ("Encountered error, scheduler drive not started.")
	}else{
		fmt.Println ("Framework Registered!")
	}
}