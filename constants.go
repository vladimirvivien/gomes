package gomes

const (
	MESOS_INTERNAL_PREFIX  = "mesos.internal."
	MESOS_SCHEDULER_PREFIX = "scheduler"
	HTTP_SCHEME            = "http"
	HTTP_POST_METHOD       = "POST"
	HTTP_MASTER_PREFIX     = "master"
	HTTP_LIBPROC_PREFIX    = "libprocess/"
	HTTP_CONTENT_TYPE      = "application/x-protobuf"
)

// calls from sched to master
const (
	REGISTER_FRAMEWORK_CALL   = "RegisterFrameworkMessage"
	UNREGISTER_FRAMEWORK_CALL = "UnregisterFrameworkMessage"
	DEACTIVATE_FRAMEWORK_CALL = "DeactivateFrameworkMessage"
	KILL_TASK_CALL            = "KillTaskMessage"
	LAUNCH_TASKS_CALL         = "LaunchTasksMessage"
)

// Events from Mesos Master
const (
	FRAMEWORK_REGISTERED_EVENT   = "FrameworkRegisteredMessage"
	FRAMEWORK_REREGISTERED_EVENT = "FrameworkReregisteredMessage"
	RESOURCE_OFFERS_EVENT        = "ResourceOffersMessage"
	RESCIND_OFFER_EVENT          = "RescindResourceOfferMessage"
	STATUS_UPDATE_EVENT          = "StatusUpdateMessage"
	FRAMEWORK_MESSAGE_EVENT      = "ExecutorToFrameworkMessage"
	LOST_SLAVE_EVENT             = "LostSlaveMessage"
)
