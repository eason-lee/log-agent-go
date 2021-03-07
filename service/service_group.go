package service

import (
	"log"
	"sync"

	"log-agent-go/utils"
)

type (
	Starter interface {
		Start()
	}

	Stopper interface {
		Stop()
	}

	Service interface {
		Starter
		Stopper
	}

	ServiceGroup struct {
		services []Service
		stopOnce func()
		routineGroup *RoutineGroup
	}
)

func NewServiceGroup() *ServiceGroup {
	sg := new(ServiceGroup)
	once := new(sync.Once)

	sg.stopOnce = func() {
		once.Do(sg.doStop)
	}
	sg.routineGroup = NewRoutineGroup()

	return sg
}

func (sg *ServiceGroup) Add(service Service) {
	sg.services = append(sg.services, service)
}

func (sg *ServiceGroup) Start() {
	utils.AddShutdownListener(func() {
		log.Println("Shutting down...")
		sg.stopOnce()
	})

	sg.doStart()
}


func (sg *ServiceGroup) DynamicAddAndStart(service Service)  {
	sg.services = append(sg.services, service)
	sg.routineGroup.RunSafe(func() {
		service.Start()
	})

}

func (sg *ServiceGroup) Stop() {
	sg.stopOnce()
}

func (sg *ServiceGroup) doStart() {
	for i := range sg.services {
		service := sg.services[i]
		sg.routineGroup.RunSafe(func() {
			service.Start()
		})
	}

	sg.routineGroup.Wait()
}

func (sg *ServiceGroup) doStop() {
	for _, service := range sg.services {
		service.Stop()
	}
}
