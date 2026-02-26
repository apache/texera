This document describes how to set up and run Texera on a single machine using "Docker Compose".

## Prerequisites

Before starting, make sure your computer meets the following requirements:

| Resource Type | Minimum | Recommended |
|-------------|---------|-------------|
| CPU Cores   | 2       | 8          |
| Memory      | 4GB     | 16GB       |
| Disk Space  | 20GB    | 50GB       |

You also need to install and launch Docker Desktop on your computer. Choose the right installation link for your computer:

| Operating System | Installation Link |
|-----------------|-------------------|
| macOS | [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/) |
| Windows | [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/) |
| Linux | [Docker Desktop for Linux](https://docs.docker.com/desktop/install/linux-install/) |

After installing and launching Docker Desktop, verify that Docker and Docker Compose are available by running the following commands from the command line:
```bash
docker --version
docker compose version
```
You should see output messages like the following (your versions may be different):
```
$ docker --version
Docker version 27.5.1, build 9f9e405
$ docker compose version
Docker Compose version v2.23.0-desktop.1
```


## Download the docker compose tarball from the release

Release candidates are available at: https://dist.apache.org/repos/dist/dev/incubator/texera/; official releases are available at: https://dist.apache.org/repos/dist/release/incubator/texera/.
 

Download the file named `apache-texera-{version}-docker-compose.tar.gz`, and then extract it.

## Launch Texera

Enter the extracted directory and run the following command to start Texera:
```bash
docker compose up
```
> If you see the error message like `unable to get image 'nginx:alpine': Cannot connect to the Docker daemon at unix:///Users/kunwoopark/.docker/run/docker.sock. Is the docker daemon running?`, please make sure Docker Desktop is installed and running

> When you start Texera for the first time, it will take around 5 minutes to download needed images.

If there are no error messages popped up, the system should be ready around 1.5 minutes. 

Open your browser and navigate to:
```
http://localhost:8080
```

Input the default account `texera` with password `texera`, and then click on the `Sign In` button to login: 
<img width="1100" height="500" alt="texera-login" src="https://github.com/user-attachments/assets/84cd784a-09a8-4e56-b9f5-49b53da67914" />


## Stop, Restart, and Uninstall Texera

### Stop
Press `Ctrl+C` in the terminal to stop Texera. 

If you already closed the terminal, you can go to the installation folder and run:
```bash
docker compose stop
```
to stop Texera.

### Restart
Same as the way you [launch Texera](#launch-texera).

### Uninstall
To remove Texera and all its data, go to the installation folder and run:
```bash
docker compose down -v
```
> ⚠️ Warning: This will permanently delete all the data used by Texera.

## Advanced Settings

Before making any of the changes below, please [stop Texera](#stop) first. Once you finish the changes, [restart Texera](#restart) to apply them.

All changes are to the `docker-compose.yml` file in the installation folder.

### Run Texera on other ports
By default, Texera uses:
- Port 8080 for its web service
- Port 9000 for its MinIO storage service

To change these ports:
- For the web service port (8080): 
  - Find the `nginx` service section and change the port mapping from `"8080:8080"` to your desired port mapping, e.g., `"8081:8080"`.
  - Find the `texera-access-message` service section and change the port in `${TEXERA_HOST}:8080` to your desired port, e.g. `${TEXERA_HOST}:8081`
- For the MinIO port (9000): 
  - Find the `minio` service section and change the port mapping from `"9000:9000"` to your desired port mapping, e.g., `"9001:9000"`.
  - Find the `lakefs` service section and update the port number of the `LAKEFS_BLOCKSTORE_S3_PRE_SIGNED_ENDPOINT` from `9000` to your desired port, e.g., `9001`.

### Change the locations of Texera data
By default, Docker manages Texera's data locations. To change them to your own locations:
- Find the `persistent volumes` section. For each data volume you want to specify, add the following configuration:
```yaml
   volume_name:
     driver: local
     driver_opts:
       type: none
       o: bind
       device: /path/to/your/local/folder
```
   For example, to change the folder of storing `workflow_result_data` to `/Users/johndoe/texera/data`, add the following:
```yaml
   workflow_result_data:
     driver: local
     driver_opts:
       type: none
       o: bind
       device: /Users/johndoe/texera/data
```

If you already launched texera and want to change the data locations, existing data volumes need to be recreated and override in the next boot-up, i.e. select `y` when running `docker compose up` again:
```
$ docker compose up
? Volume "texera-single-node-release-1-1-0_workflow_result_data" exists but doesn't match configuration in compose file. Recreate (data will be lost)? (y/N)
y // answer y to this prompt
```