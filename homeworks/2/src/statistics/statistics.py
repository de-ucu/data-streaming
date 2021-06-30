import os
import pandas as pd

INPUT_SIZE = float(os.environ['INPUT_SIZE'])
MESSAGES_COUNT = float(os.environ['MESSAGES_COUNT'])

df = pd.read_csv(f'/output/log.txt', names=["produced", "consumed"])

latency = round((df["consumed"] - df["produced"]).mean(), 2)
total_processing_time = (df["consumed"].max() - df["consumed"].min()) // 1e3
throughput_mb = round(INPUT_SIZE / total_processing_time, 2)
throughput_tweets = MESSAGES_COUNT // total_processing_time
read_time = (df["produced"].max() - df["produced"].min()) // 1e3

print("===================================================")
print(f"total read time = {read_time}s")
print(f"total processing time = {total_processing_time}s")
print(f"mean latency = {latency}ms")
print(f"throughput = {throughput_mb}Mb/s")
print(f"tweets throughput = {throughput_tweets} tweets/s")
print("===================================================")
