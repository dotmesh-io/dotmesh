FROM golang:1.12.5-alpine3.9 AS build-env
ARG VERSION
ARG STABLE_DOTMESH_SERVER_IMAGE
WORKDIR /usr/local/go/src/github.com/dotmesh-io/dotmesh
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
COPY ./cmd /usr/local/go/src/github.com/dotmesh-io/dotmesh/cmd
COPY ./pkg /usr/local/go/src/github.com/dotmesh-io/dotmesh/pkg
COPY ./vendor /usr/local/go/src/github.com/dotmesh-io/dotmesh/vendor
RUN cd cmd/operator && go install -ldflags '-linkmode external -extldflags "-static" -X main.DOTMESH_VERSION=${VERSION} -X main.DOTMESH_IMAGE=${STABLE_DOTMESH_SERVER_IMAGE}'

FROM scratch
COPY --from=build-env /usr/local/go/bin/operator /
CMD ["/operator"]