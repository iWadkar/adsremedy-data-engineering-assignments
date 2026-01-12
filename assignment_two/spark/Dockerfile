FROM apache/spark:3.5.0

ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

ENV SPARK_JARS_IVY=/tmp/.ivy2
ENV HOME=/tmp
ENV PYTHONPATH=/opt/spark/python/lib

WORKDIR /app

USER root
COPY spark.txt /app/requirements.txt
RUN pip install --no-cache-dir --target /opt/spark/python/lib -r /app/requirements.txt

COPY *.py /app/

USER spark

CMD ["/opt/spark/bin/spark-submit", "/app/etl_daily_totals.py"]
