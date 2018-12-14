# Bendo -- Server Daemon

## NAME

    `bendo` -- preservation storage daemon

## SYNOPSIS

    bendo [options]

## OPTIONS

    -config-file <PATH>

The file to read configuration options from.
If not given the default values for every option is used.
All configuration is through the config file. For the format of the configuration file
see section **CONFIG FILE** below.

## DESCRIPTION

The bendo command starts and runs the bendo service.
It will accept connections over HTTP. It writes all logging to stdout and stderr.

Bendo requires a database to run.
If the `Mysql` option is not present, an internal database engine will be used, and the
backing file will be placed in the cache directory (or kept in memory if no directory was given).


## CONFIG FILE

The config file uses the TOML file format. It consists of utf-8 text with the pound sign `#`
starting comments which continue to the end of the line. Each configuration value can be given
by using the option's name, followed by an equal sign and then the value of the option.
Strings are enclosed inside double-quote characters.

### Options

    CacheDir = "<PATH>"

Set the directory to use for storing the download cache as well as the temporary storage place for uploaded files.
If this is not given, everything is kept in memory.
The path may refer to an S3 bucket using the notation `s3:/bucket/prefix` or
`s3://hostname:port/bucket/prefix/to/use`. In this case the environment variables
`AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are used to supply the credentials
needed to access that particular S3 bucket.

    CacheSize = <MEGABYTES>

Set the maximum cache size, in megabytes (decimal, so passing "1" will set the cache size to 1,000,000 bytes, not 2**20 bytes).
This size limit applies only to the download cache, not to the temporary storage used for file uploads, so
the total space used for the cache directory may be larger than the size given.

    CacheTimeout = "<DURATION>"

If set, the time-based eviction strategy is used, and items in the cache are kept
for the given length of time since the most recent access, and then removed.
The time is reset if an item is accessed in the interim. Set the duration using
the letters "s", "m", and "h" for seconds, minutes, and hours. For example, to
set the timeout to be one day, use `"24h"`. For one month use `"720h"`, etc.
Leave empty or set to zero to use the size-based cache eviction strategy.
Defaults to 0.

    CowHost = <URL>

Setting this will enable copy-on-write mode, which cause this bendo server to mirror a second bendo server given by the URL.
This bendo server will refer to the external one whenever an item is requested which is not in
the local store. Any writes will be saved locally and not on the external bendo.
When this is enabled, background fixity checking on this bendo server is disabled (since
otherwise, all the content on the remote bendo server will end up copied to this one).
An example: `CowHost = "http://bendo.example.org:14000/"`

    CowToken = "<Token>"

Use this to give an access token to pass on when accessing the host given by the CowHost option.
If not specified, no token is used.

    Mysql = "<LOCATION>"

This will use an external MySQL database.
The parameter <LOCATION> has the form `user:password@tcp(localhost:5555)/dbname` or just `/dbname` if the
database server is on the localhost and every thing else is the default.

    PProfPort = "<PORT NUMBER>"

The port number for the pprof profiling tool to listen on. Defaults to 14001.

    PortNumber = "<PORT NUMBER>"

Gives the port number for bendo to listen on. Defaults to port 14000.

    StoreDir = "<PATH>"

The storage option provides a path to the directory in which to store the data to be preserved.
It is designed that this path maps to a networked-mapped tape system, but that is not a requirement.
It may be any disk location.
If no storage path is provided, it defaults to the current working directory.
Items are stored in uncompressed zip files having the BagIt structure, and are
organized into a two-level pairtree system.
See BAG FORMAT below for more information.

    Tokenfile = "<FILE>"

This file provides a list of acceptable user tokens.
If no file is provided all API calls to the server are unauthenticated.
The user token file should consist of a series of token lines, each separated by a new line.
A token line should give a user name, a role, and the token, in that order separated by whitespace.
The valid roles are "MDOnly", "Read", "Write", and "Admin" (case insensitive).
Empty lines and lines beginning with a hash "#" are skipped.
An example token file is

    # sample token file
    stats-logger   MDOnly   Xv78f9d9a==9034ghjVK/jfkdls+==
    batch-ingester Read     1234567890

## SIGNALS

Bendo will exit when it receives either a SIGINT or a SIGTERM.
It exits in two phases.
First, any transactions currently running are finished (but not any which are queued to run).
Second, when the already-running transactions are done, the REST API stops accepting any
new requests and any active requests are finished.
Finally, the daemon will exit.
There is a possibility that these steps may take some time to finish, on the order of minutes.

## ENVIRONMENT VARIABLES

Bendo uses a few envrionment variables to confiugure optional features.

  AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY

    These variables are used by the S3 cache store, should that be enabled by specifying an S3
    location for CacheDir in the configuration file.

  SENTRY_DSN, SENTRY_RELEASE, and SENTRY_ENVIRONMENT

    These variables contain configuration error reporting to Sentry. They are optional.
    Refer to https://docs.sentry.io/clients/go/ for information on setting them.
