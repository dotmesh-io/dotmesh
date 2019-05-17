FROM golang:1.12.5 AS build-env
WORKDIR /usr/local/go/src/github.com/dotmesh-io/dotmesh
COPY ./cmd /usr/local/go/src/github.com/dotmesh-io/dotmesh/cmd
COPY ./pkg /usr/local/go/src/github.com/dotmesh-io/dotmesh/pkg
COPY ./vendor /usr/local/go/src/github.com/dotmesh-io/dotmesh/vendor
RUN cd cmd/dotmesh-server/pkg/dind-dynamic-provisioning && go install -ldflags '-linkmode external -extldflags "-static"'

FROM scratch
COPY --from=build-env /usr/local/go/bin/dind-dynamic-provisioning /
CMD ["/dind-dynamic-provisioning"]