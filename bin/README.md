# Texera Deployment

This directory contains Dockerfiles and configuration files for building and deploying Texera's microservices.

## Dockerfiles

This directory includes several Dockerfiles, such as `file-service.dockerfile` and `computing-unit-master.dockerfile`. Each Dockerfile builds a specific Texera microservice. All Dockerfiles must be built from the `texera` project root as the Docker build context.

For example, to build the image using `texera-web-application.dockerfile`, run the following command **from the project root**:

```bash
docker build -f bin/texera-web-application.dockerfile -t your-repo/texera-web-application:test .
```

Two shell scripts, `build-images.sh` and `build-services.sh` are included for building platform-dependent images conveniently. 

You can also find prebuilt images published by the Texera team on the [Texera DockerHub Repository](https://hub.docker.com/repositories/texera).

## Deployment using images

Subdirectories `single-node` and `k8s` contain configuration files for deploying Texera using the above Docker images.