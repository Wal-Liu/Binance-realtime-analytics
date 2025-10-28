FROM apache/spark:3.4.4

USER root
RUN pip install --no-cache-dir psycopg2-binary
USER spark
