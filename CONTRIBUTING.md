# Contributing to Bendo

## Getting Started
Bendo is a Go application. There are several steps you must complete before you
can to compile, test, and commit to the project.

1. Install Go. `brew install golang`
2. Clone repository. `git clone git@github.com:ndlib/bendo.git`
3. Compile. `make`

If everything worked this will have make a few executables in the `./bin` directory.

```console
$ ls -1 ./bin
bclient
bclient-race
bendo
bstress
butil
```

## Running Bendo

A little configuration is needed before running Bendo locally.

1. Copy the `config.example` directory to a working file: `cp config.example config.local`
2. If you have MySQL running locally you can use it. Otherwise comment out the line beginning with "Mysql".
3. Comment out the "CowHost" and "CowToken" lines.
4. Comment out the "Tokenfile" file
5. Start the server with `./bin/bendo -config-file config.local`

It is running! Visit `localhost:14000` and you should see the version of the server. 
Upload content using bclient. This command line is a little complicated.

```
./bin/bclient -v -server http://localhost:14000 -ul 10 upload item1234 server
```

Then view the uploaded item at `localhost:14000/item/item1234`

# Common Tasks

## Updating vendor package

This repository uses the `go mod` tool that is a part of the standard Go toolchain.
