FROM bitnami/spark:3.5.3

USER root
RUN /opt/bitnami/python/bin/pip install --no-cache-dir psycopg2-binary

USER 1001
