# Single Node Deployment

This directory provides a Docker Compose setup for launching all core Texera microservices and dependencies on a single machine.

- `docker-compose.yml`: Main file for orchestrating all Texera microservices and third-party dependencies such as PostgreSQL, MinIO, and LakeFS.
- `nginx.conf`: Configuration file for the NGINX reverse proxy. It routes incoming HTTP requests to the correct service based on the request path.
- `.env`: Environment variables file used to configure service credentials, ports, and other runtime settings.

## Prerequisites

To launch the services, make sure:
- Docker daemon is up and running
- Port `8080`, `8000`, `9000`, and `9001` are available

## Launching the Deployment

To start all services, run the following command **from this directory**:

```bash
docker compose up 
```

This will pull the needed images from [Texera DockerHub Repository](https://hub.docker.com/repositories/texera) and launch all containers, including the web application, workflow engine, file service, and their required dependencies.

Two named volumes, `postgres` and `minio`, will be created. This ensures that the data will be persisted even if the containers are stopped or killed.

> **Note**: You should **not** modify the `.env` file. It is preconfigured with default settings, and the deployment should work out of the box.

> This architecture is intended for local testing and trial purposes only. For production deployment, use the Kubernetes-based architecture instead.

## Accessing the Application

Once the containers are up, you can access the Texera web interface at:

```
http://localhost:8080
```

LakeFS and MinIO will also be launched and can be accessed via:

```
http://localhost:8000    for lakeFS dashboard  
http://localhost:9001    for MinIO dashboard
```
