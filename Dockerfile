FROM fedora:36 as builder

# base requirements
RUN dnf install -y git clang \
    cmake e2fsprogs e2fsprogs-devel \
    protobuf-compiler protobuf-devel

RUN mkdir /rust
WORKDIR /rust

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install_rust.sh
RUN chmod +x ./install_rust.sh
RUN ./install_rust.sh -y
ENV PATH="${PATH}:/root/.cargo/bin"

WORKDIR /akula
ADD . .

RUN cargo build --workspace --profile=production

FROM fedora:36
RUN dnf install -y e2fsprogs

# Avoid copying over a bunch of junk
COPY --from=builder /akula/target/production/akula /usr/local/bin/akula
COPY --from=builder /akula/target/production/akula-rpc /usr/local/bin/akula-rpc
COPY --from=builder /akula/target/production/akula-sentry /usr/local/bin/akula-sentry
COPY --from=builder /akula/target/production/akula-toolbox /usr/local/bin/akula-toolbox
COPY --from=builder /akula/target/production/consensus-tests /usr/local/bin/consensus-tests

ARG UID=1000
ARG GID=1000

RUN groupadd -g $GID akula
RUN adduser --uid $UID --gid $GID akula
USER akula
RUN mkdir -p ~/.local/share/akula

EXPOSE 8545 \
    8551 \
    30303 \
    30303/udp \
    7545
