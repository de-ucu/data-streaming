FROM nvcr.io/nvidia/pytorch:21.02-py3

RUN python -m pip install nvidia-pyindex
RUN python -m pip install transformers tritonclient geventhttpclient confluent_kafka

WORKDIR /app
