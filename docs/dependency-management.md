# dependency management
Due to clashes with client-go and kubernetes, this project has multiple sections of vendoring:
- `/vendor` provides the vendoring for everything but tests and `cmd/dm`
- `/cmd/dm/vendor` provides vendoring for dm

To update dependencies we use `dep`, and to build we're using `bazel` (via dazel which is a version running inside a container). This means any time dependencies change (using `dep ensure -add <dependency>`/`dep ensure -update <dependency>`) you will also need to run `bazel build //:gazelle -- update-repos -from_file=Gopkg.lock` - this will need to be ran from the main directory if the change is to `/vendor` or `cmd/dm` if the change is to `/cmd/dm/vendor`

## Traps and pitfalls
- `gazelle` assumes directories beginning with `_` are to be ignored - to include these I:
  1. Renamed the dir to exclude the underscore
  1. Ran gazelle again
  1. Fixed paths in `BUILD.bazel` for that dir and any other build files which reference it.
- for some reason the openzipkin dependency tends to cause gazelle to generate a new global it can't resolve (e.g [this line](https://github.com/dotmesh-io/dotmesh/blob/master/vendor/github.com/openzipkin/zipkin-go-opentracing/BUILD.bazel#L41) is replaced with `@openzipkin_go_opentracing_crap//_thrift/files/...`). Not sure why this happens, but I have just replaced the global with the full path.
