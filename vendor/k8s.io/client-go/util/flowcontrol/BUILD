package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "backoff.go",
        "throttle.go",
    ],
    importmap = "vendor/k8s.io/client-go/util/flowcontrol",
    importpath = "",
    tags = ["automanaged"],
    deps = [
        "//github.com/juju/ratelimit:go_default_library",
        "//k8s.io/apimachinery/pkg/util/clock:go_default_library",
        "//k8s.io/client-go/util/integer:go_default_library",
    ],
)
