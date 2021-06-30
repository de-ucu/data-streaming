FROM python:3.7.10-stretch

RUN python -m pip install --upgrade pip
RUN python -m pip install spacy confluent_kafka
RUN python -m spacy download xx_ent_wiki_sm

WORKDIR /app
