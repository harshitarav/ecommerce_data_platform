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

WORKDIR /build

COPY . .

CMD ["/bin/bash"]