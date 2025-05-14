# Use an official Node runtime as a parent image
FROM node:latest

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in package.json
RUN npm install

# Define environment variable
ENV YPERSISTENCE=./leveldb
ENV HOST=0.0.0.0

# Run y-websocket server when the container launches
CMD ["npx", "y-websocket"]

# Make port 1234 available to the world outside this container
EXPOSE 1234