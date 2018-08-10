# dependency management
Due to clashes with client-go and kubernetes, this project has multiple sections of vendoring:
- `/vendor` provides the vendoring for everything excluding some clashing dependencies.
- `/cmd/dm/vendor` provides vendoring for `cmd/dm`. Eventually any dependencies which are duplicated in this repo vs `/vendor` should be removed - some of this work has already taken place.

To update dependencies we use `dep`, and to build we're using `bazel`. This means any time dependencies change, directories get added or files are moved around etc you will also need to run `bazel build //:gazelle` from the root of the repo.

## Traps and pitfalls
- `gazelle` assumes directories beginning with `_` are to be ignored - to include these I:
  1. Renamed the dir to exclude the underscore
  1. Ran gazelle again
  1. Fixed paths in `BUILD.bazel` for that dir and any other build files which reference it.
- for some reason the openzipkin dependency tends to cause gazelle to generate a new global it can't resolve (e.g [this line](https://github.com/dotmesh-io/dotmesh/blob/master/vendor/github.com/openzipkin/zipkin-go-opentracing/BUILD.bazel#L41) is replaced with `@openzipkin_go_opentracing_crap//_thrift/files/...`). Not sure why this happens, but I have just replaced the global with the full path.
