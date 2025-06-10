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
	docker-compose logs -f --tail=100

produce:
	python3 finhub_producer/producer.py

consume:
	python3 finhub_consumer/consumer.py