FROM nvcr.io/nvidia/pytorch:21.02-py3

RUN python -m pip install --upgrade pip
RUN python -m pip install transformers

ADD config.pbtxt /onnx/sentiment/config.pbtxt
ADD entrypoint.sh /scripts/entrypoint.sh
RUN ["chmod", "+x", "/scripts/entrypoint.sh"]

ENTRYPOINT ["/scripts/entrypoint.sh"]
