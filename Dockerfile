FROM python:3.8-slim
 
ENV PYTHONUNBUFFERED True

COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN apt-get update -y
RUN apt-get install -y python3-pip python-dev build-essential

RUN pip install --upgrade pip
ENV PIP_NO_CACHE_DIR=1
RUN pip install --no-cache-dir -r requirements.txt
 
COPY . /app

WORKDIR /app
 
RUN chmod 444 main.py
RUN chmod 444 requirements.txt
 
# Service must listen to $PORT environment variable.
# This default value facilitates local development.
ENV PORT 8080

EXPOSE 8080 

ENV FLASK_APP="main.py"
ENV FLASK_ENV="development"
RUN [ "flask", "assets", "build" ]

# Run the web service on container startup.
# CMD [ "python", "main.py" ]
# CMD [ "flask", "run", "--host", "8080"]

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app