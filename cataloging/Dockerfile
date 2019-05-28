FROM python:3.6

ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

COPY ./requirements.txt /cataloging/requirements.txt
RUN pip install -r /cataloging/requirements.txt

COPY . /cataloging/
WORKDIR /cataloging/

EXPOSE 8000
