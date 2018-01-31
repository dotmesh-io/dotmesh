# common-code-changes

A list of things you might need to do to the codebase and how to do it.

## adding env variables to dotmesh-server

There are CLI options passed to `dm cluster init` - for example:

  * `--image`
  * `--use-pool-dir`

These are passed down into `dotmesh-server-outer` -> `dotmesh-server-outer`
via environment variables.

These are public facing options with documentation.

There is other configuration that we don't want to expose via the CLI but
want to control using environment variables.

For example, you can pass a base64 encoded string of some JSON or YAML 
if you need complex configuration objects that affect the behavior of
`dotmesh-server-inner`.

There are two places you need to change code and then you can do one of the
following two things:

 * configure your local environment before running `dm cluster {init,upgrade}`
 * adding env variables to the k8s daemonset you are deploying

Add the name of the environment variable you want passed into 
`dotmesh-server-inner` in the following 2 places:

 * `cmd/dm/pkg/cluster.go` -> edit `inheritedEnvironment`
 * `cmd/dotmesh-server/require_zfs.sh` -> edit `INHERIT_ENVIRONMENT_NAMES`


## debugging frontend test code

Sometimes when making changes to the frontend tests - the test suite will not
boot and will not print anything useful as to why.

In this case we can debug the test code from inside the nightwatch container:

```bash
$ export TEST_DEBUG=1
$ make frontend.test
$ node specs/*.js
```

The problem is normally reported as a syntax error at this point.
