package main

import (
	"fmt"
	"code.google.com/p/goprotobuf/proto"
	"github.com/vladimirvivien/gomes"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func main() {
	master := gomes.NewMasterClient(gomes.PID("master@127.0.0.1:5050"))
	schedulerId := gomes.PID("scheduler(1)@127.0.0.1:8080")

	info := &mesos.FrameworkInfo {
		User: proto.String("test"),
		Name: proto.String("gomes"),
		Id:&mesos.FrameworkID{Value: proto.String("gomes-framework-1")},
	}

	err := master.RegisterFramework(schedulerId,*info)

	if err != nil {
		fmt.Println ("Encountered error ", err)
	}

	fmt.Println ("Hello.")
}