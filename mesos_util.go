package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
)

func NewFrameworkID(id string) *mesos.FrameworkID {
	return &mesos.FrameworkID{Value: proto.String(id)}
}

func NewFrameworkInfo(user, name string, frameworkId *mesos.FrameworkID) *mesos.FrameworkInfo {
	return &mesos.FrameworkInfo{
		User: proto.String(user),
		Name: proto.String(name),
		Id:   frameworkId,
	}
}
