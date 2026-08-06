#FROM public.ecr.aws/lambda/python:3.11
#
#RUN dnf install -y \
#    zip \
#    findutils
#
#WORKDIR /build
#
#COPY . .
#
#CMD ["/bin/bash"]

#FROM python:3.11-slim
#
#RUN apt-get update && \
#    apt-get install -y zip findutils && \
#    rm -rf /var/lib/apt/lists/*
#
#WORKDIR /build
#
#COPY . .
#
#CMD ["/bin/bash"]

FROM public.ecr.aws/sam/build-python3.11:latest

WORKDIR /build

COPY . .

CMD ["/bin/bash"]