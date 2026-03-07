# Use the python base image 
FROM python:3.12-slim

# Label maintainer as required 
LABEL maintainer="shreydhand2006@gmail.com"

# Create the application directory 
RUN mkdir /app

# Copy requirements first to leverage Docker cache on pip install 
COPY ./requirements.txt /app/requirements.txt

# Set the working directory 
WORKDIR /app

# Install dependencies from requirements.txt
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Copy the rest of the source code into the container 
COPY . /app

# Change permissions and switch to a non-privileged user 
RUN chown -R nobody:nogroup /app
USER nobody

# Document the port the service listens on (Receiver is 8080) 
EXPOSE 8100

# Set Python as the entrypoint 
ENTRYPOINT [ "python3" ]

# Default command to run your app.py 
CMD [ "app.py" ]