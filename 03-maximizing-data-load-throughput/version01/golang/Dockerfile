# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Multi-stage build
## (stage 1) build statically linked binary
FROM golang:alpine

RUN mkdir -p /go/src/app
WORKDIR /go/src/app
ADD *.go /go/src/app/
ADD vendor /go/src/app/vendor
RUN go get -d -v ./...
RUN go install -v ./...
ARG version
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o demoapp -ldflags "-X main.version=$version" 

# (stage 2) package the binary into an Alpine container
FROM alpine:3.7
RUN apk update && apk add tzdata ca-certificates && rm -rf /var/cache/apk/*
COPY --from=0 /go/src/app/demoapp .
ADD schema.sql schema.sql
ENTRYPOINT ["/demoapp"]