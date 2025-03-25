.PHONY: up down restart logs ps clean exec

up:
	docker-compose up -d

up-logs:
	docker-compose up

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f postgres

ps:
	docker-compose ps

psql:
	docker-compose exec postgres psql -U postgres

exec:
	docker-compose exec postgres $(cmd)

clean:
	docker-compose down --volumes --rmi local