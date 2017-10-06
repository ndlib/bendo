Bendo API
=========

This document describes the Bendo API. Bendo is a preservation storage system.
It presents a simple data model of *items* and *files*. The model is kept
simple so that it is easy to map the data into the next storage system,
whenever that should need to happen.

# Basic Object Types

Bendo storage consists of any number of *items*. Each item may contain *files*,
named by a path. Items are versioned, and all the files comprising an item at
each version are preserved.

Internally, files inside an item at assigned a "blob number", and this keeps
the system from keeping duplicate copies of files---files with the same hash
are only stored once per item.

# Authentication and Authorization

All authentication is via token. There may be any number of tokens, they
are read from a file at startup. Each token has a user name and a role.
The user name is used for logging and is stored when new versions of an item are uploaded. The role governs what the token is allowed to do to the server.
The roles form a strict hierarchy, and are, from least powerful to most powerful:

 * Metadata Only - this token can read metadata,
 * Reader - this token can read content + anything the Metadata Only role can do,
 * Writer - this token can write content + anything the Reader role can do,
 * Admin - this token can delete content + anything the Writer role can do.

All calls take an API key. (See API Key section)
The API key is passed in using the header `X-Api-Key`. An error of 401
Unauthorized is returned if no header is present and one is needed.
An error of 403 is returned if the key provided does not have
permission to perform the given task, is not valid, or has expired.

# Checksums

Each file inside an item will have both an MD5 checksum as well as an SHA-256
checksum. Fragments of uploaded files will have MD5 checksums. It should be
easy to add different checksums in the future as the need arises. These
checksums were chosen because they are common (as in the MD5) or secure
(SHA-256). They are used to check for transmission error (in which case they
are a bit of an overkill) and to assert provenance.

# Accounting

The following items will be tracked on each blob in the preservation system.

1. Date and time created
1. Who (the service "user") created the blob
1. Date and time deleted (if applicable)
1. Who deleted the blob (if applicable)
1. Size in bytes
1. MD5 Hash
1. SHA-256 Hash
1. Date of last checksum validation (*)
1. Result of last checksum validation (*)

All these datums are cached in the preservation system database. The
non-starred items are also stored on tape.

# API Calls

## GetContent

Routes:

    GET  /item/:item/:filepath
    GET  /item/:item/@:version/:filepath
    GET  /item/:item/@blob/:blobid
    HEAD /item/:item/:filepath
    HEAD /item/:item/@:version/:filepath
    HEAD /item/:item/@blob/:blobid

Return the content of the given file. The `filepath` may include slashes.
In this way the effect of directories for file organization can be simulated.
The forms beginning with an at sign `@` use the "extended blob syntax".

Sample paths:

    /item/abcdefg/a/path/to/a/file.txt
    /item/abcdefg/@5/a/path/to/a/file.txt
    /item/abcdefg/@blob/25

The token needs the "Reader" role for this request to succeed. (NOTE: this is
currently (March 2016) not enforced.)

There is a slight difference between the `GET` and `HEAD` form of the requests:
the `GET` request will retreive the item from tape if it is not already in
bendo's cache, whereas the `HEAD` request will **not** retreive an item from tape.
To force an item to be recalled from tape, but with no desire to read the content yet,
use a GET request and then close the connection. The file will still be cached.

For a `GET`, the response will be either the content and a 200 status code, a
206 status if a range was requested. If the item doesn't exist or the path
doesn't exit for the version specisified (not specisify a version defaults to
the newest version) a 404 response is returned. It the blob has been deleted a
410 status will be returned.

Metadata for the given blob is returned in the response headers. Some metadata
describes the blob itself, other metadata is runtime information about the
caching of the object.

 * Date and time the blob was created
 * The size of the blob
 * Whether the blob is cached on disk by the preservation system
 * The user who uploaded the blob
 * The user who deleted the blob, if the blob is deleted

One conspicuous item not tracked is the mime-type of the blob.

Request Headers:

    If-None-Match - For ETag validation
    Range - Use for range requests.
    X-Api-Key - (required)
    X-Webhook - URL to hit when the content is loaded, if the content is not cached to begin with. (not implemented)

Response Headers:

    Content-Type - bendo will try to sniff the content. This is a guess since
        bendo does not store the actual mime-type of content.
    Length - The number of bytes returned in this request.
    X-Byte-Count - Decimal integer giving total size of the blob in bytes. May be missing.
    X-Content-Md5 - The MD5 checksum of the blob, as hex digits. May be missing.
    X-Content-Sha256 - The SHA-256 checksum of the blob, as hex digits. May be missing.
    X-Cached - One of “1”, or “0”. If the blob's data was already in the cache, this will be "1", otherwise "0".
    X-Creator - Name of the API key that created this blob.
    X-Purger - Name of the API key that deleted this blob, if object is deleted. Will be missing if object is not deleted.
    Modified-Date - Date blob was uploaded or deleted in ISO-8601 format. While blobs are immutable,
        this may change if using the URLs that do not specify a version.
    Etag - An etag for this item. Since blobs are immutable, this is probably only useful for calls which do not specify a version.
    X-Fixity-Status - “ok” or “bad”. May be missing.
    X-Fixity-Date - ISO-8601 date of last fixity check for this blob. May be missing.

Errors:
    404 - No such object
    410 - Item has been deleted
    416 - Bad range request
    500 - Internal server problem

## QueryItem

Route:

    GET  /item/:item

Return information about the given item as a JSON object.
Requires the token to have the role of Metadata Only.

The information returned includes all the versions of the item.
It is possible for information about an object to not be in the
preservation system database (the system tries to keep complete information
about all objects cached but sometimes it fails). In that case this call may
block for an arbitrarly long time as the content is retreived from tape.

Response is JSON having the form (TODO: verify this)

    {
        "id": "...",
        "created" : "datetime",
        "modified" : "datetime",
        "size" : integer,
        "active-size" : integer,
        "blobs": [
            {
                "id" : "blob id",
                "created" : "datetime",
                "creator" : "username",
                "deleted" : "datetime (optional)",
                "deleter" : "username (optional)",
                "size" : 10,
                "md5" : "md5 hash (base 16)",
                "sha256" : "sha256 hash (base 16)",
                "checksum-date" : "datetime",
                "checksum-status" : "ok",
                "cached" : true,
                "url" : "..external url for this blob..",
                "file" : "..internal file location.."
                },
        ],
        "versions": [
            {
                "id" : "version number",
                "slots" : {
                    "slot-name-1" : "blob id",
                    "slot-name-2" : "blob id 2, may be empty string"
                }
            }
        ]
    }

Request Headers:

Errors:

    404 - No such item


## ListItems

Route:

    GET  /items

Return a list of item identifiers as a JSON array. User needs to have
metadataOnly role to do this.

This route is not implemented (March 2016).

## StartTransaction

Route:

    POST /item/:id/transaction

Start a new transaction on an item. Only one transaction is allowed to be open
on an item at a time.

Commits the changes listed in the given transaction, making a new version of
the item. The user needs the Writer role to do this unless the transaction
includes a “delete” command, in which case the user needs the Admin role.

Since objects are not written out to tape immediately, one cannot assume the
transfer is complete when this call returns. Returns 202 as a status if it
worked correctly.

It is intended to be used to set and alter item metadata. It is the only way to
delete blobs which already exist inside the item or to rename slots. It can
also be used to set commit notes for the current transaction.

The content of the request body contains a list of updates to make. The body
should be JSON encoded and be a list of lists. Each inside list has one of the
following forms.

    [“delete”, blobid]
Purges the underlying blob from our underlying storage. blobid is either an
integer giving the blob id, or a URL to the blob of the form /blob/:item/:blob.
It is an error to delete a blob which is being uploaded in the current
transaction.

    [“slot”, “slot name”, blobid]
Sets the given slot to point to the given blob id. The blob id may either be an
integer or a URL. URLs to blobs being uploaded in this transaction are allowed.
The blob id of 0 has the effect of removing this slot label from the next
version.

    [“note”, “text”]
Sets the transaction note to the given text.

Sample Message body:

    [
      ["delete", 56],
      ["slot", "descMetadata", “/item/1234/transaction/45a/1”],
      ["note", "blah blah"]
    ]

The user needs the Writer role to do this, unless the list includes a “delete”
command. In that case, the user needs the Admin role. Requests larger than 1 MB
are discarded.

Request Headers:

    X-Api-Key - (required)

Response Headers:

    Location - The base url for the new transaction, if a new transaction was created.

Errors:

    409 - Another transaction is already open on the item.

## ListTransactions

Route:

    GET  /transaction

List all transactions currently being tracked by the server. Old transactions
are deleted after a period of time.

## CancelTransaction

Route:

    POST /transaction/:txid/cancel

Cancels the given transaction and releases all the resources dedicated to it,
such as new blobs to add. The user needs the Writer role to call this.

## TransactionStatus

Route:

    GET  /transaction/:txid

Returns the current status of a transaction.

Errors:

    200 - the transaction has finished successfully
    204 - the transaction is still processing
    400 - There was some kind of processing error (details in the content body)


## UploadFile

Routes:

    POST /upload
    POST /upload/:fileid

Upload a new file. The initial call can be either to `/upload` or to
`/upload/:fileid`. If to the former, a random file id will be generated, and
the path is returned as the Location header. If the latter, the caller can
choose the id for the new file. If the id already exists then the request body
will be **appended** to the previous content. This behavior is intended to make
it easy to upload large files---just upload them in chunks. The data is held in
a holding area and is not saved to tape until a transaction is used to copy
this file into a particular item. The file in the holding area will be deleted
after a successful transaction, so do not reuse the file in more than one
transaction.

Files will be deleted if they are not used in a transaction in a reasonable
amount of time (probably two weeks).

The passed in checksums are for the given message body. They are checked before
saving, and a mismatch will cause an error.

Sending a request with no message body will just modify the metadata for the
blob.

The token needs to have the Writer role to call this.

Request Headers:

    Content-Length - The length of the current upload. (optional)
    X-Apikey - required
    X-Content-SHA256 - The hash for the final blob. (May be different than the
                current upload because only a part is being uploaded now).
    X-Content-MD5 - The hash for the final blob.
    X-Upload-SHA256 - The hash for the current upload in base 16 encoding. (at least one of this and X-Upload-MD5 is required)
    X-Upload-MD5 - The hash for the current upload in base 16 encoding. (at least one of this and X-Upload-SHA256 is required)

Response Headers:

    Location - The url to use for further access to this blob for the duration of this transaction.

Errors:

    400 - Checksum mismatch
    400 - missing checksum

## ListFiles

Route:

    GET  /upload

Returns a list of file ids for files in the holding area as a JSON list of
strings.
The token needs to have a Reader role to call this.


Request Headers:

    Accept-Type - use "application/json" to get JSON. otherwise HTML is returned.

## GetFile

Route:

    GET  /upload/:fileid

Returns the content of the given file in the holding area.
The token needs to have the Reader role to call this.

## RemoveFile

Route:

    DELETE /upload/:fileid

Delete a file from the holding area. The token needs to have the Writer role
to call this.

## FileMetadata

    GET  /upload/:fileid/metadata
    PUT  /upload/:fileid/metadata

These routines return and set metadata for the given file.
The token needs to have the Metadata Only role for `GET` and the Writer role
to call `PUT`.

For `PUT` the metadata is passed as a JSON object in the request body.

Metadata tracked per file:

 * `ID` - The file identifier in the holding area
 * `Size` - The size of the file (as uploaded so far to the holding area)
 * `NFragments` - The number of fragments the file. (each POST adds one more fragement)
 * `Modified` - The date the file was last modified in the holding area
 * `Created` - The date the file was created in the holding area
 * `Creator` - The name of the token which created the file
 * `MD5` - The expected MD5 checksum for the entire file
 * `SHA256` - The expected SHA256 checksum for the entire file
 * `Extra` - an arbitrary string payload, for user convinence. It is not used
by bendo.

The only field which can be altered using the `PUT` is `Extra`.
(TODO(March 2016): should also be able to change `MD5` and `SHA256`.)

## BundleAccess

Routes:

    GET  /bundle/list
    GET  /bundle/list/:prefix
    GET  /bundle/open/:key

These are the read-only low-level bundle routes. They are used to implement
the copy-on-write interface, where a second bendo server can mirror content
out of this one. They require a token with the Reader role.


## ListFixity

Route:

    GET  /fixity

Parameters:

    start
    end
    item
    status

Each fixity check either in the past or scheduled for the future has a unique
id. This endpoint provide a way to query for a list of checks. There are four
query parameters. Use `start` and `end` to limit to checks in a given time
period. The time provided should be in the form `YYYY-MM-DD`, or
`YYYY-MM-DDTHH:MM:SS`, or the wildcard `*`. If omitted, the default value is
`*` which will match any time. Use `item` to restrict to fixity checks on a
particular item. Use `status` to restrict to fixity checks with a particular
status. Possible statuses are `scheduled`, which is a pending check; `ok`,
which is a successful check; `error`, which means an error happened while
performing the check; and `mismatch`, which means there was a fixity mismatch.

Returns the results in JSON, either a list of fixity objects or `null`.

The provided API key needs Read access.

Headers:

    X-Api-Key


## GetFixity

Route:

    GET  /fixity/:id

Returns the given fixity record. A fixity record is a JSON object with the
following fields:

    ID:             the id of this record
    Item:           the item (to be) checked
    Status:         the status of this fixity check
    Scheduled_time: the time this check happened, or is scheduled to happen

The API key needs Read access.

Headers:

    X-Api-Key

## CreateFixity

Route:

    POST /fixity

Parameters:

    item
    scheduled_time

Creates a new fixity record. The parameter `item` gives the item to check. If
not provided, `scheduled_time` defaults to now. Since the background fixity
scanner only checks for more checks once an hour, it may take that long before
the check is finished.

The ID of the new check is returned in the response body.

The API key needs write access to call this endpoint.

Request Headers:

    X-Api-Key



## UpdateFixity

Route:

    PUT  /fixity/:id

Parameters:

    item
    scheduled_time

Updates the given fixity check, provided the record in the database has the
status `scheduled`. Fixity records with other status are immutable.

## DeleteFixity

Route:

    DELETE  /fixity/:id

This will remove the given fixity check, provided the record has status
`scheduled`. Records with other status cannot be deleted. One should be aware
that there is a background process that runs once a day to make sure every item
on tape has at least one pending fixity check. This makes it impossible to remove
all fixity checks for an item for more than 24 hours.


## WelcomePage

Route:

    GET  /

Return the version of the server software.

## ServerStats

Route:

    GET  /stats
    GET  /debug/vars

Return statistics on the data stored and the operational status of the server.
Requires no authentication.

Info tracked:

    * Total number of items
    * Total space used
    * Number of deleted items
    * Size of cache
    * Number of objects in each state
    * Cache hit + miss rate
    * List of items in outbound cache
    * List of items in inbound cache
    * Errors with the tape system?

This route and the information tracked may be changed in the future.


# Examples and Use Cases

## See if a file is in the cache

Given a path to a file in an item, see if it is on disk or on tape. Do not
recall it from tape should it not be in the cache.

Do a `HEAD` request to the file's URL.
The header `X-Cached` is `1` if it is in the cache,
`0` if it is not in the cache, and
`2` if it is not in the cache and will never be in the cache because it is too large.

    curl --verbose -I \
        http://bendo.example.org:14000/item/itemid/path/to/file \
        -H 'X-Api-Key: API_TOKEN'

## Recall something from tape

Warm the cache with an item from tape.

Do a `GET` request to the file's URL, but don't download any content.
The cache will be filled in the background and eventually the item will show up.
(Provided the file is not too large, which is the case when the `X-Cached` header is "2").

    curl --verbose \
        'http://bendo.example.org:14000/' \
        --max-filesize 1 \
        -H 'X-Api-Key: API_TOKEN'

## Upload a large file

Upload large files in chunks. You can determine the chunk size.
The important thing is to upload the chunks in order from first to last.
For illustration, suppose we have a 50 MB file and are uploading it as two 25 MB chunks.

This supposes we are uploading the file to the file "example" in temporary
holding area. We have already chunked the file into pieces, with the following
MD5 sums for the entire file and for each chunk:

    abcdef0123456789123  our_file
    9876543210bdcefabed  our_file.chunk1
    0246813579acefbde02  our_file.chunk2

    curl --verbose \
        'http://bendo.example.org:14000/upload/example' \
        -H 'X-Api-Key: API_TOKEN' \
        --data-binary '@our_file.chunk1' \
        -H 'X-Upload-MD5: 9876543210bdcefabed'

    curl --verbose \
        'http://bendo.example.org:14000/upload/example' \
        -H 'X-Api-Key: API_TOKEN' \
        --data-binary '@our_file.chunk2' \
        -H 'X-Upload-MD5: 0246813579acefbde02'


## Create a new item

To create a new item, first upload any files you wish to store in the item.

TODO: finish

## Delete a file (from the most current version)

## Delete a file (and purge it completely from storage)


