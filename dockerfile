FROM bitnami/spark:3.5

USER root

# Basic tools
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y \
        curl \
        gnupg \
        ca-certificates


# Add SBT repo and key, then install
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt


# install uv
RUN pip install --no-cache-dir uv
RUN apt-get update && apt-get install -y ncurses-bin

# Set working directory
WORKDIR /app

# Copy your project files
COPY pyproject.toml uv.lock /app/
RUN uv sync

# Default command
CMD ["spark-shell"]
