package gomes

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	mesos "github.com/vladimirvivien/gomes/mesosproto"
	"log"
	"os"
	"os/user"
)

type MesosError string

func NewMesosError(msg string) MesosError {
	return MesosError(msg)
}
func (err MesosError) Error() string {
	return string(err)
}

type SchedulerDriver struct {
	Master        string
	Scheduler     *Scheduler
	FrameworkInfo *mesos.FrameworkInfo
	Status        mesos.Status

	masterClient *masterClient
	schedMsgQ    chan interface{}
	controlQ     chan mesos.Status
	schedProc    *schedulerProcess
}

func NewSchedDriver(scheduler *Scheduler, framework *mesos.FrameworkInfo, master string) (*SchedulerDriver, error) {
	if master == "" {
		return nil, fmt.Errorf("Missing master address.")
	}

	if framework == nil {
		return nil, fmt.Errorf("Missing FrameworkInfo.")
	}

	// set default userid
	if framework.GetUser() == "" {
		user, err := user.Current()
		if err != nil || user == nil {
			framework.User = proto.String("unknown")
		} else {
			framework.User = proto.String(user.Username)
		}
	}

	// set default hostname
	if framework.GetHostname() == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		framework.Hostname = proto.String(host)
	}

	driver := &SchedulerDriver{
		Master:        master,
		FrameworkInfo: framework,
		schedMsgQ:     make(chan interface{}, 10),
		controlQ:      make(chan mesos.Status),
	}

	driver.Scheduler = scheduler

	proc, err := newSchedulerProcess(driver.schedMsgQ)
	if err != nil {
		return nil, err
	}

	driver.schedProc = proc

	go setupSchedMsgQ(driver)

	driver.masterClient = newMasterClient(master)

	driver.Status = mesos.Status_DRIVER_NOT_STARTED

	return driver, nil
}

func (driver *SchedulerDriver) Start() mesos.Status {
	if driver.Status != mesos.Status_DRIVER_NOT_STARTED {
		return driver.Status
	}

	// start sched proc and proc.server (http)
	err := driver.schedProc.start()
	if err != nil {
		driver.Status = mesos.Status_DRIVER_ABORTED
		driver.schedMsgQ <- err
		return driver.Status
	}

	// register framework
	err = driver.masterClient.RegisterFramework(driver.schedProc.processId, driver.FrameworkInfo)
	if err != nil {
		driver.Status = mesos.Status_DRIVER_ABORTED
		driver.schedMsgQ <- NewMesosError("Failed to register the framework:" + err.Error())
	} else {
		driver.Status = mesos.Status_DRIVER_RUNNING
	}
	return driver.Status
}

func (driver *SchedulerDriver) Join() mesos.Status {
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		return driver.Status
	}
	return <-driver.controlQ
}

func (driver *SchedulerDriver) Run() mesos.Status {
	driver.Status = driver.Start()
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		return driver.Status
	}
	return driver.Join()
}

func (driver *SchedulerDriver) Stop(failover bool) mesos.Status {
	log.Printf("Stopping framework [%s]", driver.FrameworkInfo.GetId().GetValue())
	if driver.Status != mesos.Status_DRIVER_RUNNING && driver.Status != mesos.Status_DRIVER_ABORTED {
		return driver.Status
	}
	err := driver.schedProc.stop()
	if err != nil {
		driver.schedMsgQ <- err
	}

	if driver.masterClient.connected && !failover {
		err = driver.masterClient.UnregisterFramework(driver.schedProc.processId, driver.FrameworkInfo.Id)
		if err != nil {
			driver.Status = mesos.Status_DRIVER_ABORTED
			driver.schedMsgQ <- NewMesosError("Failed to unregister the framework:" + err.Error())
		} else {
			driver.Status = mesos.Status_DRIVER_STOPPED
		}
	}

	driver.controlQ <- driver.Status // signal
	return driver.Status
}

func (driver *SchedulerDriver) Abort() mesos.Status {
	log.Printf("Aborting framework [%s]", driver.FrameworkInfo.GetId().GetValue())
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		return driver.Status
	}

	if !driver.masterClient.connected {
		log.Println("Not sending deactivate message, master is disconnected.")
	} else {
		err := driver.masterClient.DeactivateFramework(driver.schedProc.processId, driver.FrameworkInfo.Id)
		if err != nil {
			driver.schedMsgQ <- NewMesosError("Failed to abort the framework:" + err.Error())
		} else {
			driver.schedProc.aborted = true
			driver.Status = mesos.Status_DRIVER_ABORTED
		}
	}

	driver.controlQ <- driver.Status // signal
	return driver.Status

}

func setupSchedMsgQ(driver *SchedulerDriver) {
	sched := driver.Scheduler
	for event := range driver.schedMsgQ {
		if driver.Scheduler == nil {
			log.Println("WARN: Scheduler not set.")
			break
		}

		switch msg := event.(type) {
		case *mesos.FrameworkRegisteredMessage:
			go func() {
				if sched.Registered != nil {
					sched.Registered(driver, msg.FrameworkId, msg.MasterInfo)
				}
			}()

		case *mesos.FrameworkReregisteredMessage:
			go func() {
				if sched.Reregistered != nil {
					sched.Reregistered(driver, msg.MasterInfo)
				}
			}()

		case *mesos.ResourceOffersMessage:
			go func() {
				if sched.ResourceOffers != nil {
					sched.ResourceOffers(driver, msg.Offers)
				}
			}()

		case *mesos.RescindResourceOfferMessage:
			go func() {
				if sched.OfferRescinded != nil {
					sched.OfferRescinded(driver, msg.OfferId)
				}
			}()

		case *mesos.StatusUpdateMessage:
			go func() {
				if sched.StatusUpdate != nil {
					sched.StatusUpdate(driver, msg.Update.Status)
				}
			}()

		case *mesos.ExecutorToFrameworkMessage:
			go func() {
				if sched.FrameworkMessage != nil {
					sched.FrameworkMessage(driver, msg.ExecutorId, msg.SlaveId, msg.Data)
				}
			}()

		case *mesos.LostSlaveMessage:
			go func() {
				if sched.SlaveLost != nil {
					sched.SlaveLost(driver, msg.SlaveId)
				}
			}()

		case MesosError:
			go func() {
				driver.handleDriverError(msg)
			}()
		default:
			go func() {
				err := NewMesosError("Driver received unexpected event.")
				driver.handleDriverError(err)
			}()
		}
	}
}

func (driver *SchedulerDriver) handleDriverError(err MesosError) {
	if driver.Status == mesos.Status_DRIVER_ABORTED {
		log.Println("Ignoring error because driver is aborted.")
		return
	}
	stat := driver.Abort()
	if stat == mesos.Status_DRIVER_ABORTED {
		if driver.Scheduler.Error != nil {
			driver.Scheduler.Error(driver, err)
		}
	}
}
