FROM ubuntu:22.04 as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install \
       git \
       ca-certificates \
       curl \
       gcc \
       libc6-dev \
       openssl \
       libssl-dev \
       pkg-config \
       -qqy \
       --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /rust
WORKDIR /rust

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install_rust.sh
RUN chmod +x ./install_rust.sh
RUN ./install_rust.sh -y
ENV PATH="${PATH}:/root/.cargo/bin"

RUN apt-get update && apt-get install -y clang-12 libext2fs-dev wget && rm -rf /var/lib/apt/lists/*
RUN ln /usr/bin/clang-12 /usr/bin/clang

WORKDIR /protobuf
RUN wget http://ftp.debian.org/debian/pool/main/p/protobuf/libprotobuf-dev_3.20.1-1_amd64.deb http://ftp.debian.org/debian/pool/main/p/protobuf/libprotobuf-lite31_3.20.1-1_amd64.deb http://ftp.debian.org/debian/pool/main/p/protobuf/libprotoc31_3.20.1-1_amd64.deb http://ftp.debian.org/debian/pool/main/p/protobuf/libprotobuf31_3.20.1-1_amd64.deb http://ftp.debian.org/debian/pool/main/p/protobuf/protobuf-compiler_3.20.1-1_amd64.deb
RUN apt-get update && apt-get install -y ./*.deb

WORKDIR /akula
ADD . .

RUN cargo build --all --profile=production

FROM alpine:3.16.2

# Avoid copying over a bunch of junk
COPY --from=builder /akula/target/production/akula /usr/local/bin/akula
COPY --from=builder /akula/target/production/akula /usr/local/bin/akula-rpc
COPY --from=builder /akula/target/production/akula /usr/local/bin/akula-sentry
COPY --from=builder /akula/target/production/akula /usr/local/bin/akula-toolbox
COPY --from=builder /akula/target/production/akula /usr/local/bin/consensus-tests

ARG UID=1000
ARG GID=1000
RUN adduser -D -u $UID -g $GID akula
USER akula
RUN mkdir -p ~/.local/share/akula

EXPOSE 8545 \
       8551 \
       30303 \
       30303/udp \
       7545
