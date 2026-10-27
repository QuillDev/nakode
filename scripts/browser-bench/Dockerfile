FROM rust:1-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium xvfb xdotool ffmpeg fonts-dejavu-core protobuf-compiler openbox \
 && rm -rf /var/lib/apt/lists/*
RUN useradd -m bench
