package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_library",
)

go_library(
    name = "go_default_library",
    srcs = ["parsers.go"],
    importmap = "vendor/k8s.io/client-go/pkg/util/parsers",
    importpath = "",
    tags = ["automanaged"],
    deps = ["//github.com/docker/distribution/reference:go_default_library"],
)
