FROM python:3.12-slim

LABEL maintainer="shreydhand2006@gmail.com"

RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8090

ENTRYPOINT ["python3"]
CMD ["app.py"]