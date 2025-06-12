up:
	docker compose up --detach --scale spark-worker=2

down:
	docker compose down --volumes

stop:
	docker compose stop

start:
	docker compose start

restart: down up

logs:
	docker compose logs -f --tail=100

produce:
	python3 finhub_producer/producer.py

consume:
	python3 finhub_consumer/consumer.py

init-db:
	docker exec -i timescaledb psql -U postgres -d stocks < db/schema.sql

tick:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
		spark/tick_streaming.py

ohlcv:
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
	  --conf spark.pyspark.python=/home/thanhthuy/project/real-time-stock-pipeline/.venv/bin/python \
	  --conf spark.pyspark.driver.python=/home/thanhthuy/project/real-time-stock-pipeline/.venv/bin/python \
	  spark/ohlcv_streaming.py