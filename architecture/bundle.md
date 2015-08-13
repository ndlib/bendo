Bundle Format
=============

All data is written to tape in a sequence of _bundle files_. This document
describes how the bundle files are organized.

# Directory Organization

The bundle files are named in the format `XXXXX-YYYY.zip`, where XXXXX is the
curate identifier of this work item, and YYYY is a counter starting from 1. The
bundle files are organized into a two level pair-tree directory hierarchy using
the first four characters of the bundle filename. For example, the bundle file
`b4h89xw-0004.zip` would be stored at the location `b4/h8/b4h89xw-0004.zip`
relative to the base directory for the tape storage.

# Individual Bundle File

Each individual bundle file is a zip archive (TODO: in BagIt format). It
contains zero or more blob files, each in the subdirectory `blob` and a
`item-info.json` file in the root. For example:

    b4h89xw-0004.zip:
        blob/
            8
            9
            2
        item-info.json

The file `item-info.json` is a utf-8 text file containing JSON encoded data
describing this item and its serialization into the complete sequence of bundle
files. Once a bundle file is written, it is considered unchangeable, so bundle
files with larger index numbers are considered to contain information
superseding the information in lower number bundle files.

# Serialization of an Item

An item consists of a number of blobs and a sequence of versions. Blobs are
identified by integers. A given blob will usually appear in only one bundle
file. Deleted blobs will appear in no bundle files. In exceptional cases, a
blob may appear in more than one bundle. The bundle file a blob appears may
change over time due to blob deletion and bundle compaction. An index mapping
each blob to a bundle file is kept in the JSON file. A blob is in one of
four states

 * OK
 * Deleted
 * Error
 * ErrorDeleted

Most blobs should be in the OK state, which means the blob metadata is up to
date, and a correct version of the blob is available. The Deleted state means
the blob has been removed from tape; it is not available, but information about
its existence is maintained. An Error state happens if an error occured when
writing the blob to tape the first time. In this case we need to track the
incorrect blob. Error blobs are not available, and are not checksummed. Error
blobs will be deleted whenever the opportunity presents itself (namely, they
are not copied for blob deletions, effectually deleting them). ErrorDeleted is
an blob in an error state which has been deleted.

If a blob was successfully written to disk once, but an error occured while
copying it (because, say, it was in the same bundle as a blob being deleted),
it will remain in the OK state and not be in the Error state. The Error state
is only for blobs which were incompletely copied the first time, and for which
we do not have any correct data.

The `item-info.json` in the bundle with the highest sequence number is taken as
the complete truth. If the bundle with the highest sequence number is missing
this file, there is an error condition and some intervention is needed to fix
the files.

If a blob appears in more than one bundle, the version of the blob in the
bundle indicated by the item-info.json file is taken to be the correct version.

It is an error for a slot to reference a blob in the Error or ErrorDeleted
state.


## Blob deletion

A set of blobs are deleted by first identifying the set of bundle files their
canonnical versions are in. Then any other blobs in those bundle files are
copied out into a new bundle file, and the set of bundle files originally
identified are deleted.


