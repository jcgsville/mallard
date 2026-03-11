FROM rust:1.86-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

ENV TZ=Etc/UTC
ENV PATH=/home/coder/.dprint/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    jq \
    openssh-client \
    pkg-config \
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash -G sudo coder \
    && echo "coder ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/coder \
    && chmod 0440 /etc/sudoers.d/coder \
    && mkdir -p /workspaces/mallard \
    && chown -R coder:coder /workspaces/mallard

USER coder
WORKDIR /workspaces/mallard

RUN curl -fsSL https://dprint.dev/install.sh | sh
