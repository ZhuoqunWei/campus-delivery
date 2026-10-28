FROM python:3.13-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*
ENV TZ=UTC PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY delivery_analytics ./delivery_analytics
# Resolve the Kafka connector once at build time so the container starts without internet access.
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) python -c "from delivery_analytics.session import build_session; build_session('warmup').stop()" > /dev/null 2>&1
CMD ["sh", "-c", "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) exec python -m delivery_analytics.stream --bootstrap ${KAFKA_BOOTSTRAP:-kafka:9092} --output ${OUTPUT_DIR:-/data/output} --checkpoint ${CHECKPOINT_DIR:-/data/checkpoints} --starting-offsets ${STARTING_OFFSETS:-earliest} --trigger-seconds ${TRIGGER_SECONDS:-10}"]
