#!/usr/bin/env bash

SELF=$(readlink -f "$0")
DIR=$(dirname "$SELF")
DATA=$DIR/data

exec docker run --rm \
	--name wanderlust \
	--publish 127.0.0.1:5432:5432 \
	-e POSTGRES_PASSWORD=wanderlust \
	-e PGDATA=/var/lib/postgresql/data/pgdata \
	-v "$DATA:/var/lib/postgresql/data" \
	postgres "$@"
