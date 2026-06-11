---
title: "Build, Run and Configure micro‐services in local development environment"
weight: 60
---

This Document is aim to provide a instruction on how to setup the local development environment for developing and deploying the `core/micro-services`.

## Prerequisite

This document requires you to finish all the setup of Texera local development environment described in `https://github.com/Texera/texera/wiki`.

## What is `micro-services`?

`core/micro-services` is a sbt-managed project added by the PR https://github.com/Texera/texera/pull/2922. The ongoing code separation effort will gradually migrate all the services in `core/amber` to `core/micro-services`.

## How to directly build and run the micro-services directly

If you just want to run some services under `micro-services`, you can use some provided shell scripts.

### `WorkflowCompilingService`

```shell
cd texera/core

# make sure to give scripts the execution permission 
chmod +x scripts/build-workflow-compiling-service.sh
chmod +x scripts/workflow-compiling-service.sh

# Build the WorkflowCompilingService
scripts/build-workflow-compiling-service.sh

# Run the WorkflowCompilingService
scripts/workflow-compiling-service.sh
```

## How to set up the development environment

As there are many sub sbt projects under `micro-services`, Intellij is the most suitable IDE for setting up the whole environment

### Use Intellij (Most Recommended)

1. Open the folder `texera/core/micro-services` through `Open Project` in Intellij
<img width="716" alt="Screenshot 2024-11-19 at 6 00 08 PM" src="/images/github-assets/4e446332-7cfa-4974-b59b-2088a7a2d921.png">

Once you open it, Intellij will auto-detect the sbt setting and start to load the project. After loading you should see the sbt tab, which has the `micro-services` as the root project and several other services as the sub-projects:
<img width="200" alt="Screenshot 2024-11-19 at 6 05 15 PM" src="/images/github-assets/24ba1a31-1c82-4441-b525-7facc00c3ada.png">


2. Run `sbt clean compile` command in folder `core/micro-services`. This command will compile everything under `micro-services` and generate proto-specified codes.









