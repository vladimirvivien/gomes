package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"testing"
)

func TestNewFrameworkID(t *testing.T) {
	id := NewFrameworkID("test-id")
	if id == nil {
		t.Fatal("Not creating protobuf oject FrameworkID.")
	}
	if id.GetValue() != "test-id" {
		t.Fatal("Protobuf object not returning expected value.")
	}
}

func TestNewFrameworkInfo(t *testing.T) {
	info := NewFrameworkInfo("test-user", "test-name", NewFrameworkID("test-id"))
	info.Hostname = proto.String("localhost")
	if info == nil {
		t.Fatal("Not creating protobuf object FrameworkInfo")
	}
	if info.GetUser() != "test-user" {
		t.Fatal("Protobuf object FrameworkInfo.User missing value.")
	}
	if info.GetName() != "test-name" {
		t.Fatal("Protobuf object FrameworkInfo.Name missing value.")
	}
	if info.GetId() == nil {
		t.Fatal("Protobuf object FrameowrkInfo.Id missing value.")
	}
	if info.GetHostname() != "localhost" {
		t.Fatal("Protobuf object FrameworkInfo.Hostname missing value.")
	}
}

func TestNewMasterInfo(t *testing.T) {
	master := NewMasterInfo("master-1", 1234, 5678)
	if master == nil {
		t.Fatal("Not creating protobuf object MasterInfo")
	}
	if master.GetId() != "master-1" {
		t.Fatal("Protobuf object MasterInfo.Id missing.")
	}
	if master.GetIp() != 1234 {
		t.Fatal("Protobuf object MasterInfo.Ip missing.")
	}
	if master.GetPort() != 5678 {
		t.Fatal("Protobuf object MasterInfo.Port missing.")
	}
}

func TestNewOfferID(t *testing.T) {
	id := NewOfferID("offer-1")
	if id == nil {
		t.Fatal("Not creating protobuf object OfferID")
	}
	if id.GetValue() != "offer-1" {
		t.Fatal("Protobuf object OfferID.Value missing.")
	}
}

func TestNewOffer(t *testing.T) {
	offer := NewOffer(NewOfferID("offer-1"), NewFrameworkID("framework-1"), NewSlaveID("slave-1"), "localhost")
	if offer == nil {
		t.Fatal("Not creating protobuf object Offer")
	}
	if offer.GetId().GetValue() != "offer-1" {
		t.Fatal("Protobuf object Offer.Id missing")
	}
	if offer.GetFrameworkId().GetValue() != "framework-1" {
		t.Fatal("Protobuf object Offer.FrameworkId missing.")
	}
	if offer.GetSlaveId().GetValue() != "slave-1" {
		t.Fatal("Protobuf object Offer.SlaveId missing.")
	}
	if offer.GetHostname() != "localhost" {
		t.Fatal("Protobuf object offer.Hostname missing.")
	}
}

func TestNewSlaveID(t *testing.T) {
	id := NewSlaveID("slave-1")
	if id == nil {
		t.Fatal("Not creating protobuf object SlaveID")
	}
	if id.GetValue() != "slave-1" {
		t.Fatal("Protobuf object SlaveID.Value missing.")
	}
}

func TestNewTaskID(t *testing.T) {
	id := NewSlaveID("task-1")
	if id == nil {
		t.Fatal("Not creating protobuf object TaskID")
	}
	if id.GetValue() != "task-1" {
		t.Fatal("Protobuf object TaskID.Value missing.")
	}
}

func TestNewTaskStatus(t *testing.T) {
	status := NewTaskStatus(NewTaskID("task-1"), mesos.TaskState_TASK_RUNNING)
	if status == nil {
		t.Fatal("Not creating protobuf object TaskStatus")
	}
	if status.GetTaskId().GetValue() != "task-1" {
		t.Fatal("Protobuf object TaskStatus.TaskId missing.")
	}
	if status.GetState() != mesos.TaskState(mesos.TaskState_TASK_RUNNING) {
		t.Fatal("Protobuf object TaskStatus.State missing.")
	}
}
