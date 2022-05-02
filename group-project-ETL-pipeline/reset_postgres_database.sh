docker cp ./atp_data_v4.sql postgres:/atp_data_v4.sql
docker exec -it postgres psql -U postgres -a postgres -f /atp_data_v4.sql