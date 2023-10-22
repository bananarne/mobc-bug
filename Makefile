all: add-tables
	DATABASE_URL=postgres://postgres:password@localhost:5555 cargo run --release

postgres:
	docker run --name mobc-db -d -e POSTGRES_PASSWORD=password --rm -p 5555:5432 postgres

wait-for-postgres: postgres
	sleep 5

add-tables: wait-for-postgres
	docker exec -it mobc-db psql -U postgres -c 'CREATE TABLE a(c1 INTEGER, c2 INTEGER)'
	docker exec -it mobc-db psql -U postgres -c 'CREATE TABLE b(c1 INTEGER, c2 INTEGER)'
	docker exec -it mobc-db psql -U postgres -c 'CREATE TABLE c(c1 INTEGER, c2 INTEGER)'


