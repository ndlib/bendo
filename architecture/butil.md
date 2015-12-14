# Command lines for the bendo command line utility

Goal: Upload a directory of files into a bendo item on a remote bendo server.

# Usage

Suppose the server `bendo-staging.library.nd.edu:14000` contains an item named `abc123`.
This item contains the following files:

    abc123/
        README
        first.txt

## Command line 1

The first command line is

    butil upload abc123 directory-name --server bendo-staging.library.nd.edu:14000

This command will upload the files inside directory-name into the item "abc123" on the
server "bendo-staging.library.nd.edu:14000". If the directory `directory-name` contains the following files:

    directory-name/
        first.txt
        second.pdf
        extra/
            third.xls
            fourth.csv

Then the item "abc123" will be changed in the following way:

    abc123/
        README          (untouched)
        first.txt       (updated with directory-name/first.txt)
        second.pdf      (added from directory-name/second.pdf)
        extra/
            third.xls   (added from directory-name/extra/third.xls)
            fourth.csv  (added from directory-name/extra/fourth.csv)

## Command line 2

The second command line is

    butil upload abc123/extra/tenth.mp3 tenth.mp3 --server bendo-staging.library.nd.edu:14000

This command will upload the file tenth.mp3 to the item "abc123" on the server "bendo-staging.library.nd.edu:14000". The item "abc123" will now look like this

    abc123/
        README          (untouched)
        first.txt       (untouched)
        extra/
            tenth.mp3   (added)

## Command line 3

The third command line is

    butil get abc123          --server bendo-staging.library.nd.edu:14000
    butil get abc123 dest-dir --server bendo-staging.library.nd.edu:14000

This command has two variants. The first one will download the complete contents
of item "abc123" on the server "bendo-staging.library.nd.edu:14000" into the directory
"abc123" inside the current working directory (creating it if it doesn't already exist.)

    $CWD/abc123/
        README      (created from abc123/README)
        first.txt   (created from abc123/first.txt)

The second command will download the contents of the given item into the directory `dest-dir` (creating the directory if it doesn't exist).

    dest-dir/
        README      (created from abc123/README)
        first.txt   (created from abc123/first.txt)

Note, the identifier given may contain a version marker, e.g. `abc123/@2`. It may also
include a specific subdirectory or file inside the item, in which case only that subdirectory or item is downloaded. For example

    abc123/@2        - Will download version 2 of the item "abc123"
    abc123/extra     - If extra is a directory, will download only the contents of that
    abc123/README    - Will only download the file README
    abc123/@2/README - Will download the file README from version 2 of the item

# Other uses

This document has not given a way to perform the following operations on an item in a remote bendo server

 * remove a single file
 * rename a single file
 * delete a blob
 * update a file from a diff (i.e. a patching operation)

The thought was that these could be done after the essential mechanism of uploading a directory of files was finished.

# Appendix: Bendo item identifiers

A bendo item identifier is any string meeting the following conditions

 * It is at least 1 character long. (There is no specified maximum length. But we all know there is a practical maximum length. I don't know what that is).
 * It does not contain a forward slash `/` or whitespace characters.
 * It does not contain control characters.
 * TBD: should all unicode characters be allowed.

Individual files inside an item have identifiers based on the item's ID. They take
two forms:

    item-id/path/to/file
    item-id/@2/path/to/file

The first form refers to a file in the most recent version of the item. The
second form refers to a file in a specific version of an item (in this case
version 2). If a version is given which is not a positive integer or is a
version which is larger than the current version of the item, then the
identifier is invalid.
