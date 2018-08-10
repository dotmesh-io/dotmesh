package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "helpers.go",
        "register.go",
        "types.go",
    ],
    importmap = "vendor/k8s.io/client-go/tools/clientcmd/api",
    importpath = "",
    tags = ["automanaged"],
    deps = [
        "//k8s.io/apimachinery/pkg/runtime:go_default_library",
        "//k8s.io/apimachinery/pkg/runtime/schema:go_default_library",
    ],
)
