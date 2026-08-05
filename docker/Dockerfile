FROM amazonlinux:2023

RUN dnf install -y \
    python3 \
    python3-pip \
    zip

WORKDIR /build

COPY . .

CMD ["/bin/bash"]