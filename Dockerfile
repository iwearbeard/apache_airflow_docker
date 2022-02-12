# VERSION 0.0.1
# AUTHOR: Joaquin Cabada
# DESCRIPTION: Instalacion de librerias y complementos

FROM apache/airflow:2.1.3-python3.9

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt
