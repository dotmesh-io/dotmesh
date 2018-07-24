package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "client.go",
        "config.go",
        "plugin.go",
        "request.go",
        "transport.go",
        "url_utils.go",
        "urlbackoff.go",
        "versions.go",
        "zz_generated.deepcopy.go",
    ],
    importmap = "github.com/dotmesh-io/dotmesh/cmd/operator/vendor/k8s.io/client-go/rest",
    importpath = "k8s.io/client-go/rest",
    deps = [
        "//cmd/operator/vendor/github.com/golang/glog:go_default_library",
        "//cmd/operator/vendor/golang.org/x/net/http2:go_default_library",
        "//cmd/operator/vendor/k8s.io/api/core/v1:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/api/errors:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/apis/meta/v1:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime/schema:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/runtime/serializer/streaming:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/types:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/util/net:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/util/sets:go_default_library",
        "//cmd/operator/vendor/k8s.io/apimachinery/pkg/watch:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/pkg/version:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/rest/watch:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/tools/clientcmd/api:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/tools/metrics:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/transport:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/util/cert:go_default_library",
        "//cmd/operator/vendor/k8s.io/client-go/util/flowcontrol:go_default_library",
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
        "//staging/src/k8s.io/client-go/rest/fake:all-srcs",
        "//staging/src/k8s.io/client-go/rest/watch:all-srcs",
    ],
    tags = ["automanaged"],
)
