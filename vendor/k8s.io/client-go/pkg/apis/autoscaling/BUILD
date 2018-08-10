package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_library",
)

go_library(
    name = "go_default_library",
    srcs = [
        "annotations.go",
        "doc.go",
        "register.go",
        "types.go",
        "zz_generated.deepcopy.go",
    ],
    importmap = "vendor/k8s.io/client-go/pkg/apis/autoscaling",
    importpath = "",
    tags = ["automanaged"],
    deps = [
        "//k8s.io/apimachinery/pkg/api/resource:go_default_library",
        "//k8s.io/apimachinery/pkg/apis/meta/v1:go_default_library",
        "//k8s.io/apimachinery/pkg/conversion:go_default_library",
        "//k8s.io/apimachinery/pkg/runtime:go_default_library",
        "//k8s.io/apimachinery/pkg/runtime/schema:go_default_library",
        "//k8s.io/client-go/pkg/api:go_default_library",
    ],
)
