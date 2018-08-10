package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["clientauth.go"],
    importmap = "vendor/k8s.io/client-go/tools/auth",
    importpath = "",
    tags = ["automanaged"],
    deps = ["//k8s.io/client-go/rest:go_default_library"],
)
