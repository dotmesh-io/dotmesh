# Operations Guide

Operational tasks with Dotmesh

## Tracing

Dotmesh supports [OpenTracing](http://opentracing.io/) with [Zipkin](https://github.com/openzipkin/docker-zipkin).

Enable it when running `dm cluster init` and `dm cluster join` by specifying `--tracing=TRACE_HOST` where `TRACE_HOST` is the address of a Zipkin host.

It is assumed that Zipkin is running on port 9411.

You have to specify `--trace` for all `init` and `join` commands, currently it is configured per-host.

## Logging

Log to an [ELK stack](https://github.com/deviantony/docker-elk) by specifying `--log=LOG_HOST` in `dm cluster {init,join}`.

It is assumed that Logstash (or another syslog compatible log reader) is running on TCP port 5000.

## Metrics

TODO(Support sending metrics to Prometheus).

## Modifying node settings

You can use:

```
dm cluster upgrade --log=LOG_HOST --trace=TRACE_HOST
```

To modify settings without having to do the etcd node replacement dance, since this will retain the etcd data directory and the node-specific PKI assets.

## Replacing a node

Put the following alias in your `.bashrc` on one of your healthy cluster nodes:

```
alias etcdctl='docker run --net=host -ti -v /home:/home quay.io/coreos/etcd:v3.0.15 etcdctl --cert-file ~/.dotmesh/pki/apiserver.pem --key-file ~/.dotmesh/pki/apiserver-key.pem --ca-file ~/.dotmesh/pki/ca.pem --endpoints https://127.0.0.1:42379'
```

And `source ~/.bashrc` to refresh it.

Then run:

```
etcdctl member list
```

Identify the server you wish to remove and run:

```
etcdctl member remove NODE_ID
```

Now, generate a new node ID with:

```
etcdctl member add HOSTNAME https://NODE_IP:42380
```

Where `HOSTNAME` is the unique hostname of the node, and `NODE_IP` is its IP address.

Then you can take the output of `etcdctl member add`, in particular the value of the `ETCD_INITIAL_CLUSTER` variable, and run:

```
dm cluster join --etcd-initial-cluster=ETCD_INITIAL_CLUSTER https://discovery.data-mesh.io/CLUSTER_ID:SECRET
```

In this way you can replace a node which has lost its etcd data dir with a node with the same hostname/IP address.

See [etcd runtime reconfiguration](https://coreos.com/etcd/docs/latest/runtime-configuration.html#cluster-reconfiguration-operations) for more details.

## Stress testing

If you have nodes 1-4 (`node-1`, etc), run:

```
ssh node-1 docker run -v stress-test:/foo --volume-driver dm ubuntu sh -c 'echo HELLO > /foo/WORLD'
```

Then run:

```
while true;
  do for X in 1 2 3 4; do
    echo $X; ssh node-$X docker run -v stress-test:/foo \
      --volume-driver dm ubuntu cat /foo/WORLD
  done
done
```

This should run continuously.
If it doesn't, look at the UI at [http://node-1:6969/ux](http://node-1:6969/ux) and then look at the logs and fix the code.
