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

FROM public.ecr.aws/lambda/python:3.11

RUN microdnf install -y \
    zip \
    findutils \
 && microdnf clean all

WORKDIR /build

COPY . .

ENTRYPOINT []

CMD ["/bin/bash"]