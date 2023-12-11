# PoC - Apache beam

## Descripción

Creacion de un pipeline simple de transformacion y aplanamiento de un zip de registros JSON.

## Objetivos

- Instalar algun virtualenv como pipenv / poetry / conda
- Instalar beam
- Crear pipeline o pipelines con objetivo de:
  - Aplanar los JSON
  - Generar alguna métrica
  - Output en archivo parquet en carpeta data/output
- Crear proyecto propio en GCP
- Generar una cuenta de servicio para la ejecución local apache beam
- Subir datos a storage
- Mover los datos desde storage a bigquery usando apache beam local (configurar cuenta de servicio para apache beam)
- con Python enviar cada registro a Pub/Sub cada 1 seg, y con dataflow hacer streaming con estos

## Instalación
<<<<<<< HEAD

Proporciona los pasos para instalar y configurar tu proyecto. Por ejemplo:
=======
```
git clone
cd poc-apache-beam
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
pip ./scripts/beam_project.py
```
>>>>>>> 38dc7e8 (Create solution with Python and Apache Beam for data management)

## Runbook (Como replicar)


Build
```bash
docker build --platform linux/amd64 -t apache-beam-poc .
```

Run 

```bash
docker run apache-beam-poc
```


## Resources:

https://code.visualstudio.com/docs/devcontainers/containers
