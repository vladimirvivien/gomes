package gomes

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"net"
	"net/http"
	"time"
)

type masterClient struct {
	address    address
	httpClient http.Client
}

func newMasterClient(master string) *masterClient {
	return &masterClient{
		address: address(master),
		httpClient: http.Client{
			Transport: &http.Transport{
				Dial: func(netw, addr string) (net.Conn, error) {
					c, err := net.DialTimeout(netw, addr, time.Second*7)
					if err != nil {
						return nil, err
					}
					return c, nil
				},
				DisableCompression: true,
			},
		},
	}
}

func (client *masterClient) RegisterFramework(schedId schedProcID, framework *mesos.FrameworkInfo) error {
	regMsg := &mesos.RegisterFrameworkMessage{Framework: framework}
	return client.send(schedId, buildReqPath(REGISTER_FRAMEWORK_CALL), regMsg)
}

func (client *masterClient) UnregisterFramework(schedId schedProcID, frameworkId *mesos.FrameworkID) error {
	msg := &mesos.UnregisterFrameworkMessage{FrameworkId: frameworkId}
	return client.send(schedId, buildReqPath(UNREGISTER_FRAMEWORK_CALL), msg)
}

func (client *masterClient) DeactivateFramework(schedId schedProcID, frameworkId *mesos.FrameworkID) error {
	msg := &mesos.DeactivateFrameworkMessage{FrameworkId: frameworkId}
	return client.send(schedId, buildReqPath(DEACTIVATE_FRAMEWORK_CALL), msg)
}

func (client *masterClient) KillTask(schedId schedProcID, taskId *mesos.TaskID) error {
	msg := &mesos.KillTaskMessage{TaskId: taskId}
	return client.send(schedId, buildReqPath(KILL_TASK_CALL), msg)
}

func (client *masterClient) LaunchTasks(
	schedId schedProcID,
	frameworkId *mesos.FrameworkID,
	offerIds []*mesos.OfferID,
	tasks []*mesos.TaskInfo,
	filters *mesos.Filters,
) error {
	msg := &mesos.LaunchTasksMessage{
		FrameworkId: frameworkId,
		OfferIds:    offerIds,
		Tasks:       tasks,
		Filters:     filters,
	}
	return client.send(schedId, buildReqPath(LAUNCH_TASKS_CALL), msg)
}

func (client *masterClient) send(from schedProcID, reqPath string, msg proto.Message) error {
	u, err := client.address.AsHttpURL()
	if err != nil {
		return err
	}
	u.Path = reqPath

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(HTTP_POST_METHOD, u.String(), bytes.NewReader(data))
	req.Header.Add("Content-Type", HTTP_CONTENT_TYPE)
	req.Header.Add("Connection", "Keep-Alive")
	req.Header.Add("Libprocess-From", from.value)
	rsp, err := client.httpClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Master did not accept request %s.  Returned status %s.", u.String(), rsp.Status)
	}
	return nil
}

func buildReqPath(message string) string {
	return "/" + HTTP_MASTER_PREFIX + "/" + MESOS_INTERNAL_PREFIX + message
}
