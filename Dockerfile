FROM python:3.10

COPY requirements.txt /
RUN pip install -r /requirements.txt

RUN apt-get -y update
RUN apt-get install -y uuid-runtime

ENV PATH=$PATH:/src
ENV PYTHONPATH /src

COPY __init__.py /src/
COPY batch.py /src/
COPY blob_storage.py /src/
COPY batch_task.py /src/
COPY driver_helper.py /src/
COPY driver.py /src/
COPY constants.py /src/
COPY helper.py /src/
COPY model.py /src/
COPY train.py /src/
COPY predict.py /src/