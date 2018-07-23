package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["pager.go"],
    importmap = "github.com/dotmesh-io/dotmesh/cmd/operator/vendor/k8s.io/client-go/tools/pager",
    importpath = "k8s.io/client-go/tools/pager",
    deps = [
        "//cmd/operator/vendor/golang.org/x/net/context:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/api/errors:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/api/meta:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/apis/meta/internalversion:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/apis/meta/v1:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime:go_default_library",
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
    visibility = ["//visibility:public"],
)
