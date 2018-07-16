package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"log"
)

// anything to do with passing events and states back from the current state, or telling the user stuff happened

func updatePollResult(transferRequestId string, pollResult TransferPollResult) error {
	log.Printf(
		"[updatePollResult] attempting to update poll result for %s: %+v",
		transferRequestId, pollResult,
	)
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}
	serialized, err := json.Marshal(pollResult)
	if err != nil {
		return err
	}
	log.Printf(
		"[updatePollResult] => %s, serialized: %s",
		fmt.Sprintf(
			"%s/filesystems/transfers/%s",
			ETCD_PREFIX,
			transferRequestId,
		),
		string(serialized),
	)
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/filesystems/transfers/%s", ETCD_PREFIX, transferRequestId),
		string(serialized),
		nil,
	)
	if err != nil {
		log.Printf("[updatePollResult] err: %s", err)
	}
	return err
}

func (f *fsMachine) sendEvent(params *EventArgs, eventName, loggerString string) {
	log.Printf(loggerString)
	f.innerResponses <- &Event{
		Name: eventName,
		Args: params,
	}
}

func updateUser(message, transferRequestId string, pollResult TransferPollResult) error {
	pollResult.Status = "error"
	pollResult.Message = message
	return updatePollResult(transferRequestId, pollResult)
}

func (f *fsMachine) sendArgsEventUpdateUser(args *EventArgs, eventName, loggerString string, pollResult TransferPollResult) {
	f.sendEvent(args, eventName, loggerString)
	err := updateUser(loggerString, f.lastTransferRequestId, pollResult)
	if err != nil {
		f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "Cannot write to etcd")
	}
}

func (f *fsMachine) incrementPollResultIndex(
	transferRequestId string, pollResult *TransferPollResult,
) error {
	if pollResult.Index < pollResult.Total {
		pollResult.Index++
	}
	return updatePollResult(transferRequestId, *pollResult)
}

func (f *fsMachine) errorDuringTransfer(desc string, err error) stateFn {
	// for error conditions during a transfer, update innerResponses, and
	// update transfer object, and return a new state
	f.updateTransfer("error", fmt.Sprintf("%s: %s", desc, err))
	f.innerResponses <- &Event{
		Name: desc,
		Args: &EventArgs{"err": err},
	}
	return backoffState
}

func (f *fsMachine) updateTransfer(status, message string) {
	f.lastPollResult.Status = status
	f.lastPollResult.Message = message
	err := updatePollResult(f.lastTransferRequestId, *f.lastPollResult)
	if err != nil {
		// XXX proceeding despite error...
		log.Printf(
			"[updateTransfer] Error while trying to report status: %s => %s",
			message, err,
		)
	}
}
