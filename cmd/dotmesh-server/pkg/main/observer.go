// Observer pattern in golang
// from https://github.com/funkygao/golib/blob/master/observer/observer.go

package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Observer struct {
	events  map[string][]chan interface{}
	rwMutex sync.RWMutex
}

func NewObserver() *Observer {
	return &Observer{
		rwMutex: sync.RWMutex{},
		events:  map[string][]chan interface{}{},
	}
}

func (o *Observer) Subscribe(event string, outputChan chan interface{}) {
	o.rwMutex.Lock()
	o.events[event] = append(o.events[event], outputChan)
	o.rwMutex.Unlock()
}

func (o *Observer) String() string {
	o.rwMutex.RLock()
	defer o.rwMutex.RUnlock()
	s := []string{}
	for k, v := range o.events {
		s = append(s, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(s, " ")
}

// Stop observing the specified event on the provided output channel
func (o *Observer) Unsubscribe(event string, outputChan chan interface{}) error {
	o.rwMutex.Lock()
	defer o.rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	var outChans []chan interface{}

	outChans, ok := o.events[event]
	if !ok {
		outChans = newArray
	}
	for _, ch := range outChans {
		if ch != outputChan {
			newArray = append(newArray, ch)
		} else {
			close(ch)
		}
	}

	o.events[event] = newArray
	return nil
}

// Stop observing the specified event on all channels
func (o *Observer) UnsubscribeAll(event string) error {
	o.rwMutex.Lock()
	defer o.rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	var outChans []chan interface{}

	outChans, ok := o.events[event]
	if !ok {
		outChans = newArray
	}

	for _, ch := range outChans {
		close(ch)
	}
	delete(o.events, event)

	return nil
}

/*
  A note about the semantics of Publish:

  As it creates a goroutine to send to each subscriber (to avoid the
  loop being blocked if subscribers don't receive from their
  unbuffered channels in a timely manner), it doesn't guarantee that
  messages Publish()ed actually arrive at the subscribers in the order
  they were sent.

  So: Make no assumption about the order messages arrive in.
*/

func (o *Observer) Publish(event string, data interface{}) error {
	o.rwMutex.Lock()
	defer o.rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	var outChans []chan interface{}

	outChans, ok := o.events[event]
	if !ok {
		outChans = newArray
		o.events[event] = outChans
	}

	// notify all through chan
	for _, outputChan := range outChans {
		go func(outputChan chan interface{}) {
			defer func() {
				// recover from panic caused by writing to a closed channel, caused by Unsubscribe racing with Publish
				// (see issue https://github.com/dotmesh-io/dotmesh/issues/53 )
				if r := recover(); r != nil {
					return
				}
			}()
			outputChan <- data
		}(outputChan)
	}

	return nil
}

func (o *Observer) PublishTimeout(event string, data interface{}, timeout time.Duration) error {
	o.rwMutex.Lock()
	defer o.rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	var outChans []chan interface{}

	outChans, ok := o.events[event]
	if !ok {
		outChans = newArray
		o.events[event] = outChans
	}

	for _, outputChan := range outChans {
		select {
		case outputChan <- data:
		case <-time.After(timeout):
		}
	}

	return nil
}
