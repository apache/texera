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


By default, Texera services require ports **8080** and **9000** to be free. If either port is already in use, the services will fail to start.

On macOS or Linux, run the following commands to check:

```
lsof -i :8080
lsof -i :9000
```

If either command produces output, that port is occupied by another process. You will need to either stop that process or change Texera's port configuration. See [Advanced Settings > Run Texera on other ports](#run-texera-on-other-ports) for instructions.

---


## Launch Texera

Enter the extracted directory and run the following command to start Texera:
```bash
docker compose --profile examples up
```

This command will start docker containers that host the  Texera services, and pre-create two example workflows and datasets.

If you don't want to have these examples pre-created, run the following command instead:
```bash
docker compose up
```

> If you see the error message like `unable to get image 'nginx:alpine': Cannot connect to the Docker daemon at unix:///Users/kunwoopark/.docker/run/docker.sock. Is the docker daemon running?`, please make sure Docker Desktop is installed and running

> When you start Texera for the first time, it will take around 5 minutes to download needed images.


The system should be ready around 1.5 minutes. After seeing the following startup message:
```
...
=========================================
  Texera has started successfully!
  Access at: http://localhost:8080
=========================================
...
```

you can open the browser and navigate to the URL shown in the message.

Input the default account `texera` with password `texera`, and then click on the `Sign In` button to login:
<img width="1100" height="500" alt="texera-login" src="https://github.com/user-attachments/assets/84cd784a-09a8-4e56-b9f5-49b53da67914" />


## Stop, Restart, and Uninstall Texera

### Stop
Press `Ctrl+C` in the terminal to stop Texera.

If you already closed the terminal, you can go to the installation folder and run:
```bash
docker compose --profile examples stop
```
to stop Texera.

### Restart
Same as the way you [launch Texera](#launch-texera).

### Uninstall
To remove Texera and all its data, go to the installation folder and run:
```bash
docker compose --profile examples down -v
```
> ⚠️ Warning: This will permanently delete all the data used by Texera.


## Enable the Texera Agent

The Texera agent is powered by a large language model (LLM). By default, Texera uses [Claude Haiku 4.5](https://www.anthropic.com/claude/haiku) as the LLM and queries it through [LiteLLM](https://docs.litellm.ai/). Without an API key, the Texera agent panel still appears but model calls will fail with a provider auth error.

To enable it:

1. [Stop Texera](#stop) if it is already running.
2. Get an API key for the LLM. Since Claude Haiku 4.5 is enabled by default, you need an [Anthropic API key](https://console.anthropic.com/settings/keys).
3. Export the key and restart Texera:
   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   docker compose --profile examples up
   ```

Once Texera is up, create a new workflow and open the Texera agent panel at the bottom right. Type a task like:

> For /texera/popular-movies-of-imdb/v1/TMDb_updated.csv, visualize the top 10 most-voted movies.

To switch providers or add more LLMs, see [Add more LLMs or providers](#add-more-llms-or-providers).



## Advanced Settings

Before making any of the changes below, please [stop Texera](#stop) first. Once you finish the changes, [restart Texera](#restart) to apply them.

All changes below are to the `.env` file in the installation folder, unless otherwise noted.

### Run Texera on other ports
By default, Texera uses:
- Port 8080 for its web service
- Port 9000 for its MinIO storage service

To change these ports, open the `.env` file and update the corresponding variables:
- For the web service port (8080): change `TEXERA_PORT=8080` to your desired port, e.g., `TEXERA_PORT=8081`.
- For the MinIO port (9000): change `MINIO_PORT=9000` to your desired port, e.g., `MINIO_PORT=9001`.

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

### Add more LLMs or providers
Only Claude Haiku 4.5 is enabled by default. To add more LLMs, open `litellm-config.yaml` in the installation folder and append entries under `model_list`. Each entry follows this shape:
```diff
  model_list:
    ...
+   - model_name: <name shown in Texera>
+     litellm_params:
+       model: <provider model id>
+       api_key: "os.environ/<API_KEY_ENV_VAR>"
```
For example, to add OpenAI's GPT-5.2 and Google's Gemini 2.5 Pro:
```diff
  model_list:
    ...
+   - model_name: gpt-5.2
+     litellm_params:
+       model: gpt-5.2
+       api_key: "os.environ/OPENAI_API_KEY"
+
+   - model_name: gemini-2.5-pro
+     litellm_params:
+       model: gemini/gemini-2.5-pro
+       api_key: "os.environ/GEMINI_API_KEY"
```
Make sure to set the corresponding API key environment variable when you launch Texera (see [Enable the Texera Agent](#enable-the-texera-agent)). Get keys from each provider's console — for example, [OpenAI](https://platform.openai.com/api-keys) or [Google](https://aistudio.google.com/apikey).

If your provider is not Anthropic, OpenAI, or Google, also pass its key into the LiteLLM container by editing `docker-compose.yml`:
```diff
  litellm:
    ...
    environment:
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY:-}
      OPENAI_API_KEY: ${OPENAI_API_KEY:-}
      GEMINI_API_KEY: ${GEMINI_API_KEY:-}
+     <NEW_API_KEY>: ${<NEW_API_KEY>:-}
```

For the full list of supported providers and model IDs, see the [LiteLLM proxy config docs](https://docs.litellm.ai/docs/providers).

## Troubleshooting

### Port conflicts

If Texera fails to start, a common cause is that ports 8080 or 9000 are already in use by another application. Check which ports are occupied:

```
lsof -i :8080
lsof -i :9000
```

Stop the conflicting process, or change Texera's ports following the instructions in [Advanced Settings > Run Texera on other ports](#run-texera-on-other-ports).

### Volume conflicts

PostgreSQL only runs the database initialization scripts on first startup (when its data volume is empty). If you previously started Texera and then ran `docker compose down` (without `-v`), the data volume still exists. On the next `docker compose up`, the initialization is skipped, which can cause services like lakeFS to fail because their required databases were never created.

To resolve this, remove all existing volumes and start fresh:

```
docker compose --profile examples down -v
docker compose --profile examples up
```

> ⚠️ Warning: `docker compose --profile examples down -v` permanently deletes all Texera data.
