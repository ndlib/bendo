#!/bin/bash

# Start and configure the bendo service.
# This script is only intended for use in a development environment.

bendo_dir="$HOME/goapps/bendo"

echo "Ensuring presence of needed directories"
mkdir -p "$bendo_dir"/{bendo_cache,bendo_storage}

config_file="$bendo_dir"/development.config
if [ -f "$config_file" ]; then
  echo "Config file already exists"
else
  echo "Creating a default config file"
  sed "/Mysql/d" "$GOPATH"/src/github.com/ndlib/bendo/config.example > "$config_file"
fi

token_file="$bendo_dir"/Tokenfile
if [ -f "$token_file" ]; then
  echo "Token file already exists"
else
  echo "Creating a default Tokenfile"
  cp "$GOPATH"/src/github.com/ndlib/bendo/Tokenfile.example "$token_file"
fi

echo "Starting service"
cd "$bendo_dir" && bendo -config-file development.config
