# Texera Deployment

This directory contains Dockerfiles and configuration files for building and deploying Texera's microservices.

## Dockerfiles and Images

This directory includes several Dockerfiles, such as `file-service.dockerfile` and `computing-unit-master.dockerfile`. Each Dockerfile builds a specific Texera microservice. All Dockerfiles must be built from the `texera` project root as the Docker build context.

For example, to build the image using `texera-web-application.dockerfile`, run the following command from the `deployment` directory:

```bash
docker build -f texera-web-application.dockerfile -t texera/texera-web-application:test ..
``` 

You can also find prebuilt images published by the Texera team on the [Texera DockerHub Repository](https://hub.docker.com/repositories/texera).

## Deployment Based on Docker Images

Subdirectories like `single-node` contain configuration files for deploying Texera using the above Docker images.

### `single-node` architecture

The `single-node` directory includes the necessary files for deploying Texera on a single machine using Docker Compose.
