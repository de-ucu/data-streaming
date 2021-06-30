FROM python:3.7.10-stretch

RUN python -m pip install --upgrade pip
RUN python -m pip install fasttext confluent_kafka

RUN curl https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin --create-dirs -o /model/model.bin

WORKDIR /app
