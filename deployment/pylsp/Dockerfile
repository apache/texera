# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install pylsp
RUN pip install python-lsp-server[all]==1.5.0 python-lsp-server[websockets] pylint==2.15.10
RUN apt update
RUN apt install -y htop

# Copy the bash script into the container
COPY run_pylsp.sh /usr/src/app/run_pylsp.sh

# Make the script executable
RUN chmod +x /usr/src/app/run_pylsp.sh

# Set the entrypoint to the script
ENTRYPOINT ["/usr/src/app/run_pylsp.sh"]

# Run pylsp on container startup
# ENTRYPOINT ["pylsp", "--ws", "--port", "3000", "--verbose", "--check-parent-process"]
