FROM python:3.11-slim

WORKDIR /mqtt-exporter

RUN pip3 install --upgrade pip
COPY main.py ./
COPY __init__.py ./
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 9000

ENTRYPOINT [ "python3" ]
CMD [ "./main.py" ]