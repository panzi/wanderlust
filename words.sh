#!/usr/bin/bash

set -eo pipefail

SELF=$(readlink -f "$0")
DIR=$(dirname "$SELF")

cd "$DIR"

{
    echo '#![allow(non_upper_case_globals)]'
    echo
    sed 's/.*/pub const &: \&str = "&";/' < words.txt
} > src/model/words.rs
