package main

import (
	"fmt"
	"log"
)

// anything to do with passing events and states back from the current state, or telling the user stuff happened

func (f *fsMachine) sendEvent(params *EventArgs, eventName, loggerString string) {
	log.Printf(loggerString)
	f.innerResponses <- &Event{
		Name: eventName,
		Args: params,
	}
}

func (f *fsMachine) updateUser(message string) error {
	f.transferUpdates <- TransferUpdate{
		Kind: TransferStatus,
		Changes: TransferPollResult{
			Status:  "error",
			Message: message,
		},
	}
	return nil
}

func (f *fsMachine) sendArgsEventUpdateUser(args *EventArgs, eventName, loggerString string) {
	f.sendEvent(args, eventName, loggerString)
	err := f.updateUser(loggerString)
	if err != nil {
		f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "Cannot write to etcd")
	}
}

func (f *fsMachine) incrementPollResultIndex() error {
	f.transferUpdates <- TransferUpdate{
		Kind:    TransferIncrementIndex,
		Changes: TransferPollResult{},
	}

	return nil
}

func (f *fsMachine) errorDuringTransfer(desc string, err error) stateFn {
	// for error conditions during a transfer, update innerResponses, and
	// update transfer object, and return a new state
	f.updateUser(fmt.Sprintf("%s: %s", desc, err))
	f.innerResponses <- &Event{
		Name: desc,
		Args: &EventArgs{"err": err},
	}
	return backoffState
}

func (f *fsMachine) updateTransfer(status, message string) {
	f.transferUpdates <- TransferUpdate{
		Kind: TransferStatus,
		Changes: TransferPollResult{
			Status:  status,
			Message: message,
		},
	}
}
