FROM golang:1.12.5-alpine3.9 AS build-env
WORKDIR /usr/local/go/src/github.com/dotmesh-io/dotmesh
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
COPY ./cmd /usr/local/go/src/github.com/dotmesh-io/dotmesh/cmd
COPY ./pkg /usr/local/go/src/github.com/dotmesh-io/dotmesh/pkg
COPY ./vendor /usr/local/go/src/github.com/dotmesh-io/dotmesh/vendor
RUN cd cmd/dynamic-provisioner && go install -ldflags '-w -s'

FROM scratch
COPY --from=build-env /usr/local/go/bin/dynamic-provisioner /
CMD ["/dynamic-provisioner"]