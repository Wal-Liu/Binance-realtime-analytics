FROM spark:3.5.0-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary pandas pyarrow

RUN pip uninstall protobuf -y
RUN pip install protobuf==3.20.3 --force-reinstall --no-cache-dir
USER spark
