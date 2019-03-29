package types

type RPCForkRequest struct {
	MasterBranchID string
	ForkNamespace  string
	ForkName       string
}
