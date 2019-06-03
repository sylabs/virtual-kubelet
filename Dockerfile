FROM golang:1.12 as builder
ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go
COPY . /go/src/github.com/virtual-kubelet/virtual-kubelet
WORKDIR /go/src/github.com/virtual-kubelet/virtual-kubelet
ARG BUILD_TAGS=""
RUN CGO_ENABLED=0 go build -tags slurm_provider -o main cmd/virtual-kubelet/main.go

FROM scratch
COPY --from=builder /go/src/github.com/virtual-kubelet/virtual-kubelet/main /app/
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs
WORKDIR /app
ENTRYPOINT [ "./main" ]
CMD [ "--help" ]
