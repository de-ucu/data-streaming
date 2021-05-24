#### 1. docker ps -a
![alt text](images/1.jpg "Завдання 1")

#### 4. Create a topic
```kafka-topics --create --bootstrap-server broker_1:9092 --topic test_topic_1```

![alt text](images/4.jpg "Завдання 4")

#### 5. List topics
```kafka-topics --list --bootstrap-server broker_1:9092```

![alt text](images/5.jpg "Завдання 5")

#### 6. Sent at least 10 simple text messages with the console producer
```
    kafka-console-producer --topic test_topic_1 --bootstrap-server broker_1:9092 --property key.separator=, --property parse.key=true
    >Ukraine,Kyiv
    >Ukraine,Sumy
    >Ukraine,Lviv
    >Ukraine,Donetsk
    >Switzerland,Bern
    >Switzerland,Geneva
    >Austria,Vienna
    >Poland,Warsaw
    >Lithuania,Vilnius
    >Portugal,Lisbon
```

![alt text](images/6.jpg "Завдання 6")

#### 7. Receive the messages with the console  consumer
`kafka-console-consumer --topic test_topic_1 --bootstrap-server broker_1:9092 --property print.key=true --from-beginning`

![alt text](images/7.jpg "Завдання 7")

#### 8. Delete the topic
`kafka-topics --delete --bootstrap-server broker_1:9092 --topic test_topic_1`

![alt text](images/8.jpg "Завдання 8")
