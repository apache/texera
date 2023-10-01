from flask import Flask, jsonify
import docker
import subprocess
import time
from my_function import container_start_finished
import threading

app = Flask(__name__)

request_lock = threading.Lock()

client = docker.from_env()
#Container subject to change depending on docker env. Change as needed
container_name_or_id = "docker_texera_1"
end_string = "---------Now we have 1 nodes in the cluster [current default #worker per operator=2]---------"

global_time = 0

#Subject to change depending on required time
container_time_limit = 300 

def start_container(container_name):
    global global_time
    return_string = ""
    try:
        request_lock.acquire()
        container = client.containers.get(container_name)
        if container.status == "exited":
            command = ["docker-compose", "-f", "core/docker/docker-compose.yml", "up", "-d"]
            subprocess.run(command, check=True)
            global_time = time.time()
            container_start_finished(end_string, container)
            time.sleep(3)
            return_string = "Docker Compose project started successfully."
        else:
            return_string = "Container running"
    except subprocess.CalledProcessError as e:
        return_string = f"An error occurred: {e}"
    except Exception as e:
        return_string = "An error has occurred!"
    finally:
        if request_lock.locked():
            request_lock.release()
        return return_string


# Function to stop a container
def stop_container(container_name):
    global global_time
    return_string = ""
    try:
        container = client.containers.get(container_name)
        if container.status == "running":
            command = ["docker-compose", "-f", "core/docker/docker-compose.yml", "stop"]
            subprocess.run(command, check=True)
            global_time = 0
            return_string = "Docker Compose project stopped successfully"
        else:
            raise(ValueError("Container already stopped"))
    except subprocess.CalledProcessError as e:
        return_string = f"An error occurred: {e}"
    except ValueError as e:
        return_string = f"{e}"
    except Exception as e:
        return_string = "An error has occurred!"
    finally:
        if request_lock.locked():
            request_lock.release()
        return return_string


def container_monitor():
    while True:
        try:
            # Get the current time and container status outside of the lock
            current_time = time.time()
            container = client.containers.get(container_name_or_id)
            container_status = container.status

            # Check if a container is running
            if container_status == "running":
                if global_time > 0 and current_time - global_time > container_time_limit:
                    # Automatically stop the container
                    stop_container(container_name_or_id)

            # Sleep for a short interval before checking again
            time.sleep(10)

        except Exception as e:
            print(f"Error in container monitor: {e}")


# Start the container monitor thread
container_monitor_thread = threading.Thread(target=container_monitor)
container_monitor_thread.daemon = True
container_monitor_thread.start()

@app.route('/start')
def start_docker_compose_endpoint():
    return jsonify(start_container(container_name_or_id))

@app.route('/stop')
def stop_docker_compose_endpoint():
    return jsonify(stop_container(container_name_or_id))

@app.route('/refresh')
def refresh_docker_compose_endpoint():
    request_lock.acquire()
    global global_time
    if global_time != 0:
        global_time = time.time() 
    if request_lock.locked():
        request_lock.release()
    return jsonify("Successful")

#App deployment subject to change depending on ports available and deployment type. Change as needed
if __name__ == '__main__':
    app.run(debug=True)
