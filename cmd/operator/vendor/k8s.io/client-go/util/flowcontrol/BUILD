package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "backoff.go",
        "throttle.go",
    ],
    importmap = "github.com/dotmesh-io/dotmesh/cmd/operator/vendor/k8s.io/client-go/util/flowcontrol",
    importpath = "k8s.io/client-go/util/flowcontrol",
    deps = [
        "//cmd/operator/vendor/github.com/juju/ratelimit:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/util/clock:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/util/integer:go_default_library",
    ],
)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [":package-srcs"],
    tags = ["automanaged"],
)
