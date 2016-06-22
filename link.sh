#!/bin/bash

# Link the binaries built by the Makefile to $GOPATH/bin

echo "Checking to see if \$GOPATH is set"
"${GOPATH:?GOPATH must be non-empty}"

echo "Looking for $GOPATH/bin"
if [ -d "$GOPATH/bin" ]; then
  echo "$GOPATH/bin directory exists"
else
  echo "Creating $GOPATH/bin directory"
  mkdir -p "$GOPATH/bin"
fi

echo "Linking binaries"
ln -s $GOPATH/src/github.com/ndlib/bendo/bin/* "$GOPATH/bin"
