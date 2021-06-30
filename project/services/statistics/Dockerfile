FROM python:3.7.10-stretch

RUN python -m pip install --upgrade pip
RUN python -m pip install pandas jupyterlab matplotlib seaborn

ADD ./src/statistics.ipynb /app/statistics.ipynb

ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--notebook-dir=/app", "--allow-root"]
