# Apache Airflow 🚀

### Despliegue 📦

1) _Crear imagen de Docker_

```
docker build . -f Dockerfile -t pip-python:2.3.0
```

2) _Levantar docker-compose con todos los servicios_

```
docker-compose up -d
```

## Control de contenedores ⚙️

_Status_

```
docker ps
```

_Logs_

```
docker logs <<id_contenedor>>
```

## Construido con 🛠️

Python, Apache Airflow y Docker

* [Apache Airflow](https://airflow.apache.org/) - El framework usado
* [Docker](https://www.docker.com//) - Herramienta de contenedores

## Autor ✒️

* **Joaquin Alvarez** - [jalvarezcabada](https://github.com/jalvarezcabada)
