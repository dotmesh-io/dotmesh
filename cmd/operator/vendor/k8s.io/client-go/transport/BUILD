package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "cache.go",
        "config.go",
        "round_trippers.go",
        "transport.go",
    ],
    importmap = "github.com/dotmesh-io/dotmesh/cmd/operator/vendor/k8s.io/client-go/transport",
    importpath = "k8s.io/client-go/transport",
    deps = [
        "//cmd/operator/vendor/github.com/golang/glog:go_default_library",
        "//cmd/operator/vendor/github.com/gregjones/httpcache:go_default_library",
        "//cmd/operator/vendor/github.com/gregjones/httpcache/diskcache:go_default_library",
        "//cmd/operator/vendor/github.com/peterbourgon/diskv:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/util/net:go_default_library",
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
    srcs = [
        ":package-srcs",
        "//staging/src/k8s.io/client-go/transport/spdy:all-srcs",
    ],
    tags = ["automanaged"],
)
