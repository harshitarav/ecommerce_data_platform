FROM amazonlinux:2023

RUN dnf install -y \
    python3 \
    python3-pip \
    zip \
    findutils

WORKDIR /build

COPY . .

CMD ["/bin/bash"]