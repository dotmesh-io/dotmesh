package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_library",
)

go_library(
    name = "go_default_library",
    srcs = ["latest.go"],
    importmap = "github.com/dotmesh-io/dotmesh/cmd/operator/vendor/k8s.io/client-go/tools/clientcmd/api/latest",
    importpath = "k8s.io/client-go/tools/clientcmd/api/latest",
    deps = [
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime/schema:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime/serializer/json:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime/serializer/versioning:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/tools/clientcmd/api:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/tools/clientcmd/api/v1:go_default_library",
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
