Bendo
=====

Bendo is a front-end to our tape system.
It provides an object store, a REST interface, groups small files into larger bundles,
and does fixity checking.
It saves data into the filesystem, which is an NFS mounted partition of our storage appliance.

* To read about Bendo design....link.
* To read about how to get started...link.
* To read about configuration...link.
* To get started hacking...link.

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

# Deployment in Production

TBD

# Contributions

TBD

# TODO

 * Update README to have
   - list of procedures for development, testing
   - list of any external tooling
   - directions on how to set up a development environment
   - directions on how to set up a remote copy-on-write
   - information on user tokens
