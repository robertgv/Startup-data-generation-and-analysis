FROM python:latest
WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
CMD ["python", "-u", "app.py"]