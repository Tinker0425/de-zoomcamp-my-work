# copy paste to base terminal window
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

# pgcli from base
pip install pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi
