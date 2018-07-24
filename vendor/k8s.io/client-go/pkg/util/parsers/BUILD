package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_library",
)

go_library(
    name = "go_default_library",
    srcs = ["parsers.go"],
    importmap = "github.com/dotmesh-io/dotmesh/vendor/k8s.io/client-go/pkg/util/parsers",
    importpath = "k8s.io/client-go/pkg/util/parsers",
    tags = ["automanaged"],
    deps = ["//vendor/github.com/docker/distribution/reference:go_default_library"],
)
