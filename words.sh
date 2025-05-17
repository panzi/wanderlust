#!/usr/bin/bash

set -eo pipefail

SELF=$(readlink -f "$0")
DIR=$(dirname "$SELF")

cd "$DIR"

exec sed 's/.*/pub const &: \&str = "&";/' < words.txt > src/model/words.rs
