FROM python:3.10-slim
WORKDIR /app
COPY nod.py server.py client.py /app/
ENTRYPOINT ["python", "-u", "nod.py"]