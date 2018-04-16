# Hacking on the operator

Set up a place shared with the dind containers:

```
mkdir /dotmesh-test-pools/operator
chmod a+rwx /dotmesh-test-pools/operator
```

Compile it:

```
CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o /dotmesh-test-pools/operator/operator .
```

Run it:

```
dex 0 0 /dotmesh-test-pools/operator/operator --kubeconfig=/root/.kube/config -v 2
```

`-v 3` gives messier logging. No `-v` at all makes it only log when it actually does something interesting.
