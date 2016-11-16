Bendo
=====

[![APACHE 2
License](http://img.shields.io/badge/APACHE2-license-blue.svg)](./LICENSE)
[![Contributing
Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

Bendo is a front-end to our tape system.
It provides an object store, a REST interface, groups small files into larger bundles,
and does fixity checking.
It saves data into the filesystem, which is an NFS mounted partition of our storage appliance.

* To read about Bendo design....link.
* To read about how to get started...link.
* To read about configuration...link.
* To get started hacking...link.

# Elevator Pitch

Bendo is a content agnostic storage service.
It provides the abstraction of a versioned directory, similar to the Moab storage system or Git.
Every version of every file stored in it has a unique URI.
It serves the same purpose as Moab, except instead of keeping each file in the filesystem, it collects them into Bags.
Each version of an item has its own bag (more or less).
It uses bags since our tape system dislikes many small files, which is what we have with all the metadata files we have.
It will run fixity checks on the content in the background.
It also provides some caching of content.
It was designed to fit into a larger digital architecture.

# Description of this repository

This repository contains the code for the Bendo server, along with related command-line tools, tests, and documentation.
The server is in `cmd/bendo`.

# Getting Started

To install bendo, first install golang. This is probably easy with a package manager, e.g. `brew install go` or `yum install golang`.

Then install the bendo server by executing `go get github.com/ndlib/bendo/cmd/bendo`.
In the directory of your hydra application, create a subdirectory to store files, for example `bendo`

    mkdir -p bendo/uploads bendo/store

Then start bendo

    bendo --storage-dir bendo/store --uploads bendo/uploads &

This will run bendo in the background on port 14000. You can test it by hitting `localhost:14000` in your browser and seeing the bendo version displayed.

If you already had files in these directories Bendo will resync itself on them, but it may take some time. (How will one know when it is finished?)

# Copy-On-Write

It is possible for a bendo server to pull content from a second bendo server.
In this way, the first bendo server will appear to have all the content the second one has, but any writes or changes to the data are kept
only in the first one.
The transfer of data happens in the background, and is not noticiable to any clients.
The ability is only a proof-of-concept now, and entire bundle files are transferred.
If the Copy-on-Write ability is useful, the code should be rewritten so that
only individual blobs are transferred between the two bendo servers.

Enable COW mode by setting the http address for the second bendo server in the config file.
If the second bendo server is protected by a token, also give an access token.

    CowHost = "http://bendo.example.com:14000"
    CowToken = "1234567890"

The second bendo server supports the copying by default and does not need to be configured in any way.

# Deployment in Production

TBD

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
