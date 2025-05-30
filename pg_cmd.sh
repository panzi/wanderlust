#!/usr/bin/env bash

exec docker exec -u postgres --tty --interactive wanderlust "$@"
