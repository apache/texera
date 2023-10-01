import time
import docker

def container_start_finished(endstring, container):
    count = 0
    current_timestamp_seconds = int(time.time())
    while True:
        if count <= 20:
            print("Checking logs\n__________________________\n")
            logs = container.logs(stdout=True, stderr=True, stream=False, timestamps=True, tail=1, since=current_timestamp_seconds, follow=False)
            for log_entry in logs.splitlines():
                string_input = log_entry.decode("utf-8")
                print(string_input)
                if endstring in string_input:
                    return 
            time.sleep(5)
            count += 1
        else:
            raise ValueError("Unexpected Container Behavior")