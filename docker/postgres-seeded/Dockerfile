FROM postgres:16

# This image is "seeded" with an existing database dump.
# The restore runs only on FIRST startup (empty PGDATA).

COPY seed/house_warehouse.dump /docker-entrypoint-initdb.d/seed.dump
COPY initdb/10_restore.sh /docker-entrypoint-initdb.d/10_restore.sh

RUN chmod +x /docker-entrypoint-initdb.d/10_restore.sh
