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
	connected    bool
	failover     bool
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
		Status:        mesos.Status_DRIVER_NOT_STARTED,
		schedMsgQ:     make(chan interface{}, 10),
		controlQ:      make(chan mesos.Status),
		connected:     false,
		failover:      false,
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
	if driver.Status != mesos.Status_DRIVER_RUNNING {
		return driver.Status
	}
	err := driver.schedProc.stop()
	if err != nil {
		driver.schedMsgQ <- err
	}

	if driver.connected && !failover {
		err = driver.masterClient.UnregisterFramework(driver.schedProc.processId, driver.FrameworkInfo.Id)
		if err != nil {
			driver.Status = mesos.Status_DRIVER_ABORTED //TODO confirm logic
			driver.schedMsgQ <- NewMesosError("Failed to unregister the framework:" + err.Error())
		} else {
			driver.Status = mesos.Status_DRIVER_STOPPED
			driver.connected = false // assume disconnection.
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

	if !driver.connected {
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
		if sched == nil {
			log.Println("WARN: Scheduler not set, no event will be handled.")
		}

		switch msg := event.(type) {
		case *mesos.FrameworkRegisteredMessage:
			driver.handleRegistered(msg)

		case *mesos.FrameworkReregisteredMessage:
			driver.handleReregistered(msg)

		case *mesos.ResourceOffersMessage:
			driver.handleResourceOffers(msg)

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
				driver.handleError(msg)
			}()
		default:
			go func() {
				err := NewMesosError("Driver received unexpected event.")
				driver.handleError(err)
			}()
		}
	}
}

func (driver *SchedulerDriver) handleRegistered(msg *mesos.FrameworkRegisteredMessage) {
	if driver.Status == mesos.Status_DRIVER_ABORTED {
		log.Println("Ignoring FrameworkRegisteredMessage, the driver is aborted!")
		return
	}

	if driver.connected == true {
		log.Println("Ignoring FrameworkRegisteredMessage, the driver is already connected!")
		return
	}

	//TODO detect if message was from leading-master (sched.cpp)

	log.Printf("Framework registered with ID [%s] ", msg.GetFrameworkId().GetValue())

	// TODO add synchronization
	driver.connected = true
	driver.failover = false

	sched := driver.Scheduler
	if sched != nil && sched.Registered != nil {
		go sched.Registered(driver, msg.FrameworkId, msg.MasterInfo)
	}
}

func (driver *SchedulerDriver) handleReregistered(msg *mesos.FrameworkReregisteredMessage) {
	if driver.Status == mesos.Status_DRIVER_ABORTED {
		log.Println("Ignoring FrameworkReRegisteredMessage, the driver is aborted!")
		return
	}

	if driver.connected == true {
		log.Println("Ignoring FrameworkReRegisteredMessage, the driver is already connected!")
		return
	}

	//TODO detect if message was from leading-master (sched.cpp)

	log.Printf("Framework re-registered with ID [%s] ", msg.GetFrameworkId().GetValue())

	// TODO add synchronization
	driver.connected = true
	driver.failover = false

	sched := driver.Scheduler
	if sched != nil && sched.Reregistered != nil {
		sched.Reregistered(driver, msg.MasterInfo)
	}
}

func (driver *SchedulerDriver) handleResourceOffers(msg *mesos.ResourceOffersMessage) {
	if driver.Status == mesos.Status_DRIVER_ABORTED {
		log.Println("Ignoring ResourceOffersMessage, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.Println("Ignoring ResourceOffersMessage, the driver is not connected!")
		return
	}

	sched := driver.Scheduler
	if sched != nil && sched.ResourceOffers != nil {
		go sched.ResourceOffers(driver, msg.Offers)
	}

}

func (driver *SchedulerDriver) handleError(err MesosError) {
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
