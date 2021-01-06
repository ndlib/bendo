Bendo
=====

[![APACHE 2 License](http://img.shields.io/badge/APACHE2-license-blue.svg)](./LICENSE)
[![Contributing Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)
[![Go Report Card](https://goreportcard.com/badge/github.com/ndlib/bendo)](https://goreportcard.com/report/github.com/ndlib/bendo)

Bendo is a content agnostic storage service.
It provides services useful to digital preservation and an API on top ofour tape system,
and is designed to be a piece of a larger digital library architecture.
Bendo provides the abstraction of a versioned directory, similar to the [Moab](https://journal.code4lib.org/articles/8482) storage design or Git.
Every version of every file it stores has a unique URI.
All files are collected into uncompressed Zip files in the [BagIt format](https://tools.ietf.org/html/draft-kunze-bagit-16).
The created Zip files are treated as immutable, and are never changed once created.
Updates and deletions are handled by adding more Zip files.
Content is deduplicated within each item, so unchanged files do not need to be duplicated when new versions are created. 

Bendo runs periodic fixity checks on content.
It also caches content, so popular items do not need to be read from tape every time.
It can store the cache either on disk or in S3.

* [Read about the file system organization](architecture/bundle.md)
* [API documentation](architecture/api.md)
* [Daemon manpage](architecture/cmd_bendo.md)


# Description of this repository

This repository contains the code for the Bendo server along with related command-line tools, tests, and documentation.
The repository is organized as so:

 * `cmd/bendo` is the top-level application
 * `cmd/bclient` is a command line utility to interact with a Bendo server
 * `server` contains everything relating with the REST API and databases
 * `blobcache` is the cache logic
 * `transaction` for the code to create and update items
 * `items` for reading and writing the stored bundle files
 * `bagit`, `fragment`, `store` handle details with file format, storage, and organization
 * `architecture` has some design documents and other guides
 * `bclientapi` has supporting code for the `bclient` utility

# Getting Started

To install Bendo, first install Go. This is probably easiest with a package manager, e.g. `brew install go` or `yum install golang`.

There are instructions in [./CONTRIBUTING.md]() for installing and running Bendo locally.

# S3

Bendo can use S3 as storage for the cache. To use it specify the bucket name and an optional prefix
to use by setting the `CacheDir` to be `s3:/bucket/prefix`.
Put the credentials in the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`.

You can use a local instance of Minio as well. For example, using docker:

    docker run -p 9000:9000 -e "MINIO_ACCESS_KEY=bob" -e "MINIO_SECRET_KEY=1234567890" minio/minio server /data

Then set the `CacheDir` to access this server by supplying a host name:

    CacheDir = s3://localhost:9000/bucket/prefix

And set the environment variables to have the correct access key and secret access key.
To run the S3 tests in the `store/` directory run

    env "AWS_ACCESS_KEY_ID=bob" "AWS_SECRET_ACCESS_KEY=1234567890" go test -tags=s3 -run S3

# BlackPearl

Bendo can use the SpectraLogic BlackPearl appliance as a storage location. To
configure it give a storage dir location in the form of `blackpearl://[hostname
or IP address]:[port]/bucket/prefix`. Or use `blackpearls://` if the device is
configured to use https for its API. Put the credentials in the environment
variables `DS3_ACCESS_KEY` and `DS3_SECRET_KEY`. The blackpearl support
requires enough temporary drive space to store the largest file being uploaded
or 1 GB, whichever is larger. By default the system temp file directory is
used. To change this, give an alternate directory in the `DS3_TEMPDIR`
environment variable.


# Sentry

Bendo can optionally send error messages to the Sentry service. Enable it by setting the environment
variables `SENTRY_DSN`, `SENTRY_RELEASE`, and `SENTRY_ENVIRONMENT`.


# Copy-On-Write

It is possible for one Bendo server to pull content from a second Bendo server.
In this way, the first Bendo server will appear to have all the content the second one has,
but any writes or changes to the data are kept only in the first one.
The transfer of data happens in the background, and is not noticeable to any clients.
The ability is only a proof-of-concept now, and entire bundle files are transferred.
If the Copy-on-Write ability is useful, the code should be rewritten so that
only individual blobs are transferred between the two Bendo servers.

Enable COW mode by setting the http address for the second Bendo server in the configuration file.
If the second Bendo server is protected by a token, also give an access token.

    CowHost = "http://bendo.example.com:14000"
    CowToken = "1234567890"

The second Bendo server supports the copying by default and does not need to be configured in any way.


# Deployment in Production

TBD

# Codebuild and the Docker buildimage container

You can configure codebuild to build RPM images using a docker image to do the compiling.
The Dockerfile is `docker/buildimage/Dockerfile`.
You can rebuild it locally using `make buildimage` in the root of the repository.

Update the container image in Docker Hub by running `make upload-buildimage` in the root of the repository.

# Contributions

Structure imports like:

    import (
        // standard library packages

        // other external packages

        // ndlib packages
    )

Before committing, run `go fmt` on the repository.
We also use `go vet` and `golint` occasionally, but for now they are not required on each check-in.

# Releasing New Versions

1. Add the new version to the CHANGELOG.md along with a summary of the changes
   since the previous release. (A high level summary with attention paid to any
   gotchas when upgrading).
2. Commit the edits
3. Tag the git repo with the new version using `git tag -a v2016.1 -m 'Tag
   v2016.1'` where each occurrence of "v2016.1" is replaced with the version
   number. I've been prefixing the git tags for versions with a lower case "v".

# TODO

 * Update README to have
   - list of procedures for development, testing
   - list of any external tooling
   - directions on how to set up a development environment
   - directions on how to set up a remote copy-on-write
   - information on user tokens
