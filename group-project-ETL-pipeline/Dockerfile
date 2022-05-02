FROM postgres
COPY atp_data_v4.sql /docker-entrypoint-initdb.d/
RUN ["sed", "-i", "s/exec \"$@\"/echo \"skipping...\"/", "/usr/local/bin/docker-entrypoint.sh"]
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV PGDATA=/data

RUN ["/usr/local/bin/docker-entrypoint.sh", "postgres"]


FROM bitnami/spark

USER root

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /opt/bitnami/spark/jars/postgresql-42.2.18.jar && \
    curl https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.10/mongo-java-driver-3.12.10.jar -o /opt/bitnami/spark/jars/mongo-java-driver-3.12.10.jar && \
    curl https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar -o /opt/bitnami/spark/jars/mongo-spark-connector_2.12-3.0.1.jar && \
    python3 -m pip install --no-cache-dir pandas selenium webdriver_manager datetime fake_useragent tqdm

RUN python3 -m pip uninstall dotenv
RUN python3 -m pip uninstall python-dotenv
RUN python3 -m pip install python-dotenv

RUN apt-get update
RUN apt-get install -y wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb