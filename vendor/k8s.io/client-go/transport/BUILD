package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "cache.go",
        "config.go",
        "round_trippers.go",
        "transport.go",
    ],
    importmap = "vendor/k8s.io/client-go/transport",
    importpath = "",
    tags = ["automanaged"],
    deps = [
        "//github.com/golang/glog:go_default_library",
        "//k8s.io/apimachinery/pkg/util/net:go_default_library",
    ],
)
