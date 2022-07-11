# VERSION 0.0.1
# AUTHOR: Joaquin Cabada
# DESCRIPTION: Instalacion de librerias y complementos

FROM apache/airflow:2.3.0-python3.9

USER root

RUN apt-get -y update \
    && apt-get -y install git

COPY requirements.txt .

USER airflow

RUN python3 -m pip install --upgrade pip \
    && pip3 install --user -r requirements.txt
