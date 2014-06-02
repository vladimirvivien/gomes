package main

import (
	"fmt"
	"code.google.com/p/goprotobuf/proto"
	"github.com/vladimirvivien/gomes"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func main() {
	master := gomes.newMasterClient("127.0.0.1:5050")
	schedulerId := gomes.newSchedID("127.0.0.1:50505")

	framework := &mesos.FrameworkInfo {
			User: proto.String("test"),
			Name: proto.String("gomes"),
			Id:&mesos.FrameworkID{Value: proto.String("gomes-framework-1")},
		},
	}

	err := master.RegisterFramework(schedulerId,*framework)

	if err != nil {
		fmt.Println ("Encountered error ", err)
	}

	fmt.Println ("Framework Registered!")
}