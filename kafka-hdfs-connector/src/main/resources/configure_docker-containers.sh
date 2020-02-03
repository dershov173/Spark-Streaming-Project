docker run --name kafka_cluster -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
           -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=127.0.0.1 \
           -v /Users/dershov/IdeaProjects/Spark-Streaming-Project/kafka-hdfs-connector/target/kafka-hdfs-connector-1.0-SNAPSHOT-jar-with-dependencies.jar:/connectors/kafka-hdfs-connector.jar \
           -e RUNTESTS=0 lensesio/fast-data-dev:2.0.1

docker network create -d bridge --subnet 172.25.0.0/16 mynetwork
docker network connect mynetwork griffin
docker network connect mynetwork kafka_cluster