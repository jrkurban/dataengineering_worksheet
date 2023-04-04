## 1. Copy/push project to VM

## 2. Fix sh files
```commandline
sed -i 's/\r$//' spark/spark-master.sh
sed -i 's/\r$//' spark/spark-worker.sh
```
## 3. Docker compose
```commandline
docker-compose up --build -d
```
- Wait for the minio is healthy

## 4. Create bucket, user with mc
```commandline
# Connect spark-client
docker exec -it spark-client bash

# Add user spark
mc admin user add dataops_minio spark Ankara06

# Set spark readwrite
mc admin policy set dataops_minio readwrite user=spark

# Create a bucket named datasets
mc mb dataops_minio/datasets

# Download dataset
curl -o iris.csv https://raw.githubusercontent.com/erkansirin78/datasets/master/iris.csv --insecure

# Upload dataset to bucket
mc cp iris.csv dataops_minio/datasets

# List datasets
mc ls dataops_minio/datasets
[2022-05-24 15:36:22 UTC] 4.5KiB STANDARD iris.csv
```

## 5. Open MinIO Web UI
http://localhost:9001

- dataops - Ankara06
- See datasets bucket

## 6. Submit spark code
- In spark-client container
```commandline
spark-submit --master spark://spark-master:7077 /opt/examples/simple_submit.py
```

- A little while later you will see
```commandline
+-------------+------------+-------------+------------+-----------+
|SepalLengthCm|SepalWidthCm|PetalLengthCm|PetalWidthCm|    Species|
+-------------+------------+-------------+------------+-----------+
|          5.1|         3.5|          1.4|         0.2|Iris-setosa|
|          4.9|         3.0|          1.4|         0.2|Iris-setosa|
|          4.7|         3.2|          1.3|         0.2|Iris-setosa|
|          4.6|         3.1|          1.5|         0.2|Iris-setosa|
|          5.0|         3.6|          1.4|         0.2|Iris-setosa|
|          5.4|         3.9|          1.7|         0.4|Iris-setosa|
|          4.6|         3.4|          1.4|         0.3|Iris-setosa|
|          5.0|         3.4|          1.5|         0.2|Iris-setosa|
|          4.4|         2.9|          1.4|         0.2|Iris-setosa|
|          4.9|         3.1|          1.5|         0.1|Iris-setosa|
|          5.4|         3.7|          1.5|         0.2|Iris-setosa|
|          4.8|         3.4|          1.6|         0.2|Iris-setosa|
|          4.8|         3.0|          1.4|         0.1|Iris-setosa|
|          4.3|         3.0|          1.1|         0.1|Iris-setosa|
|          5.8|         4.0|          1.2|         0.2|Iris-setosa|
|          5.7|         4.4|          1.5|         0.4|Iris-setosa|
|          5.4|         3.9|          1.3|         0.4|Iris-setosa|
|          5.1|         3.5|          1.4|         0.3|Iris-setosa|
|          5.7|         3.8|          1.7|         0.3|Iris-setosa|
|          5.1|         3.8|          1.5|         0.3|Iris-setosa|
+-------------+------------+-------------+------------+-----------+
only showing top 20 rows
```

## 7. Spark Master UI
http://localhost:8080/

