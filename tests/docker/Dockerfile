FROM mirantis/kubeadm-dind-cluster:v1.7
RUN cd /tmp && mkdir cni-plugins && cd cni-plugins && \
    curl -sSL -o cni-plugins-amd64-v0.6.0-rc1.tgz https://github.com/containernetworking/plugins/releases/download/v0.6.0-rc1/cni-plugins-amd64-v0.6.0-rc1.tgz && \
    tar zxfv cni-plugins-amd64-v0.6.0-rc1.tgz && \
    cp /tmp/cni-plugins/* /opt/cni/bin/
COPY dindnet /usr/local/bin/dindnet
COPY cni.conflist /etc/cni/net.d/01-weave-hostport.conflist
