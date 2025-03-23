# Single Node Deployment

This directory provides a Docker Compose setup for launching all core Texera microservices and dependencies on a single machine.

## Prerequisites
- Docker daemon is up and running
- Port `8080`, `8000`, `9000` and `9001` are available

## Launching the Deployment

To start all services, run the following command from this directory:

```bash
docker compose up
```

This will build and launch all containers, including the web application, workflow engine, file service, and their required dependencies. 

Two named volumes, `postgres` and `minio`, will be created. This ensures that the data will be persisted even if the containers are stopped or killed.

## Accessing the Application

Once the containers are up, you can access the Texera web interface at:

```
http://localhost:8080
```

LakeFS and Minio will also be launched and can be accessed via:

```
http://localhost:8000    for lakeFS dashboard
http://localhost:9001    for minio dashboard
```

## File Descriptions

- `docker-compose.yml`: Main file for orchestrating all Texera microservices and third-party dependencies such as PostgreSQL, MinIO, and LakeFS.
- `nginx.conf`: Configuration file for the NGINX reverse proxy. It routes incoming HTTP requests to the correct service based on the request path.
- `.env`: Environment variables file used to configure service credentials, ports, and other runtime settings.
