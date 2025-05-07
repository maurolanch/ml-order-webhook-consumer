FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py .

CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]