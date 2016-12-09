Bundle Format
=============

Each item is stored on tape in a sequence of _bundle files_. This document
describes how the bundle files are organized.

# Directory Organization

A bundle file has a name in the format `XXXXX-YYYY.zip`, where XXXXX is the
item identifier, and YYYY is a zero-padded integer sequence number greater than
or equal to 1. The bundle files are organized into a two level pair-tree
directory hierarchy using the first four characters of the bundle filename. For
example, the bundle file `b4h89xw-0004.zip` would be stored at the location
`b4/h8/b4h89xw-0004.zip` relative to the base directory for the tape storage.

If a sequence number should pass 9999, then the file name is expanded to use
five digits for the number, and a sixth digit is added when sequence numbers
pass 100,000.

# Individual Bundle File

Each individual bundle file is a zip archive in the BagIt format. The files are
stored uncompressed inside the zip file. In addition to the BagIt required
manifests and tag files, each bag contains a metadata file describing the
entire item in `data/item-info.json` and zero or more blob files, each named by
its blob identifier and stored in the subdirectory `data/blob`. An item may be
serialized across many bundle files, and each bundle file should contain as
complete of an metadata record as possible for the entire item, and not just
the files in that particular bundle file. A new bundle file should have a
sequence number greater (numerically) than any previously written bundle file
for that item. Once a bundle file is written, it is considered unchangeable, so
bundle files with larger index numbers are considered to contain information
superseding the information in lower number bundle files.

For example, inside the `b4h89xw-0004.zip` bundle file, we would find the
following file hierarchy

    b4h89xw-0004.zip:
        data/
            blob/
                8
                9
                2
            item-info.json
        bag-info.txt
        bagit.txt
        manifest-md5.txt
        manifest-sha256.txt
        tagmanifest-md5.txt

The file `item-info.json` is a utf-8 text file containing JSON encoded data
describing this item and its serialization into the complete sequence of bundle
files. It contains all the information needed to reconstruct the item from its
bundle files. It has a list of versions, and for each version a mapping from
paths to blob identifiers. It also has a list of blobs, and for each blob a
mapping from blob identifier to the bundle file that contains it.

An example `item-info.json` would look like the following.

    {
      "ItemID": "bendo",
      "ByteCount": 14806273,
      "Versions": [
        {
          "VersionID": 1,
          "SaveDate": "2015-09-29T14:50:32.079237902-04:00",
          "Creator": "db",
          "Note": "",
          "Slots": {
            "main.go": 1
          }
        },
        {
          "VersionID": 2,
          "SaveDate": "2015-09-29T17:03:04.161355379-04:00",
          "Creator": "db",
          "Note": "",
          "Slots": {
            "main.go": 2
          }
        }
      ],
      "Blobs": [
        {
          "BlobID": 1,
          "Bundle": 1,
          "ByteCount": 14805524,
          "MD5": "a90734ff27b18c020c9885b5e72e3633",
          "SHA256": "0588cacce12e9a56e0d4ef7d4b713c27ea9855a640223c8727ec2fb5fa68be63",
          "SaveDate": "2015-09-29T17:03:03.678727469-04:00",
          "Creator": "db",
          "DeleteDate": "0001-01-01T00:00:00Z",
          "Deleter": "",
          "DeleteNote": ""
        },
        {
          "BlobID": 2,
          "Bundle": 2,
          "ByteCount": 749,
          "MD5": "83a945af5173197f9e9a383fb98cc20e",
          "SHA256": "7000a38bd350158e6dccf20454aacf87a05115db5fadc3712a9ce22ef5335daf",
          "SaveDate": "2015-09-29T17:03:04.148039931-04:00",
          "Creator": "db",
          "DeleteDate": "0001-01-01T00:00:00Z",
          "Deleter": "",
          "DeleteNote": ""
        }
      ]
    }


# Serialization of an Item

An item consists of a number of blobs and a sequence of versions. Blobs are
identified by integers. A given blob will usually appear in only one bundle
file. Deleted blobs will appear in no bundle files. In exceptional cases, a
blob may appear in more than one bundle. The bundle file assigned to a blob may
change over time due to blob deletion and bundle compaction. The metadata file
keeps an index mapping each blob to its bundle file.

The `item-info.json` in the bundle with the highest sequence number is taken as
the complete truth. If the bundle with the highest sequence number is missing
this file, there is an error condition and some intervention is needed to fix
the files.

If a blob appears in more than one bundle, the version of the blob in the
bundle indicated by the item-info.json file is taken to be the correct version.


## Blob deletion

A set of blobs are deleted by first identifying the set of bundle files
containing their canonical versions. Then for each identified bundle file, all
the other blobs in that file are copied into a new bundle file, and then, if
there were no errors, the set of identified bundle files are deleted.

# Bundle Verification

Items can be verified at both the item level and the bundle level. Bundle level
verification is performed at the BagIt level, which means each bundle file is
tested to see if it can be opened, and then all the files inside are compared
against their hashes in the BagIt manifest file. Also the files listed in
the bag manifest should correspond one-to-one with the payload files in the
bag.

At the item level, the metadata JSON file is read, checked for correctness, and
the blobs are verified to be inside the bundle file so indicated in the
metadata. The metadata is also verified for inconsistent dates or missing
entries.
