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

func NewMasterInfo(id string, ip, port uint32) *mesos.MasterInfo {
	return &mesos.MasterInfo{
		Id:   proto.String(id),
		Ip:   proto.Uint32(ip),
		Port: proto.Uint32(port),
	}
}

func NewOfferID(id string) *mesos.OfferID {
	return &mesos.OfferID{Value: proto.String(id)}
}

func NewOffer(offerId *mesos.OfferID, frameworkId *mesos.FrameworkID, slaveId *mesos.SlaveID, hostname string) *mesos.Offer {
	return &mesos.Offer{
		Id:          offerId,
		FrameworkId: frameworkId,
		SlaveId:     slaveId,
		Hostname:    proto.String(hostname),
	}
}

func NewSlaveID(id string) *mesos.SlaveID {
	return &mesos.SlaveID{Value: proto.String(id)}
}

func NewTaskID(id string) *mesos.TaskID {
	return &mesos.TaskID{Value: proto.String(id)}
}

func NewTaskStatus(taskId *mesos.TaskID, state mesos.TaskState) *mesos.TaskStatus {
	return &mesos.TaskStatus{
		TaskId: &mesos.TaskID{Value: proto.String("test-task-1")},
		State:  mesos.TaskState(state).Enum(),
	}
}

func NewStatusUpdate(frameworkId *mesos.FrameworkID, taskStatus *mesos.TaskStatus, timestamp float64, uuid []byte) *mesos.StatusUpdate {
	return &mesos.StatusUpdate{
		FrameworkId: frameworkId,
		Status:      taskStatus,
		Timestamp:   proto.Float64(timestamp),
		Uuid:        uuid,
	}
}
