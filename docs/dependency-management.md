# dependency management
Due to clashes with client-go and kubernetes, this project has multiple sections of vendoring:
- `/vendor` provides the vendoring for everything but tests and `cmd/dm`
- `/cmd/dm/vendor` provides vendoring for dm
- `/tests/vendor` provides vendoring for unit tests.

To update dependencies we use `dep`, and to build we're using `bazel` (via dazel which is a version running inside a container). This means any time dependencies change (using `dep ensure -add <dependency>`/`dep ensure -update <dependency>`) you will also need to run `bazel build //:gazelle -- update-repos -from_file=Gopkg.lock` - this will need to be ran from the main directory if the change is to `/vendor` or `cmd/dm` if the change is to `/cmd/dm/vendor`

## Traps and pitfalls
- `gazelle` assumes directories beginning with `_` are to be ignored - to include these I renamed them to not include it, put the underscores back then changed the paths referenced in `BUILD.bazel` wherever folders like that are included.
