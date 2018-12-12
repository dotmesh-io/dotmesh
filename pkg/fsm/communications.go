package fsm

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

// anything to do with passing events and states back from the current state, or telling the user stuff happened

func (f *FsMachine) sendEvent(params *types.EventArgs, eventName, loggerString string) {
	log.Printf(loggerString)
	f.innerResponses <- &types.Event{
		Name: eventName,
		Args: params,
	}
}

func (f *FsMachine) updateUser(message string) error {
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStatus,
		Changes: types.TransferPollResult{
			Status:  "error",
			Message: message,
		},
	}
	return nil
}

func (f *FsMachine) sendArgsEventUpdateUser(args *types.EventArgs, eventName, loggerString string) {
	f.sendEvent(args, eventName, loggerString)
	err := f.updateUser(loggerString)
	if err != nil {
		f.sendEvent(&types.EventArgs{"err": err}, "cant-write-to-etcd", "Cannot write to etcd")
	}
}

func (f *FsMachine) incrementPollResultIndex() error {
	f.transferUpdates <- types.TransferUpdate{
		Kind:    types.TransferIncrementIndex,
		Changes: types.TransferPollResult{},
	}

	return nil
}

func (f *FsMachine) errorDuringTransfer(desc string, err error) StateFn {
	// for error conditions during a transfer, update innerResponses, and
	// update transfer object, and return a new state
	f.updateUser(fmt.Sprintf("%s: %s", desc, err))
	f.innerResponses <- &types.Event{
		Name: desc,
		Args: &types.EventArgs{"err": err},
	}
	return backoffState
}

func (f *FsMachine) updateTransfer(status, message string) {
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStatus,
		Changes: types.TransferPollResult{
			Status:  status,
			Message: message,
		},
	}
}
