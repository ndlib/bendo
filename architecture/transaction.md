Transaction Spec
================

This document describes how the transaction part of the REST API shall work.

The transaction piece is the most complicated since it needs to allow clients
to perform many different operations on items. It allows new files to be added,
files inside the item to be renamed, file entries to be removed, and files to
be deleted. It also allows for setting commit notes which are stored in the
item's version history.

## Format

A commands are uploaded as JSON encoded strings. They consist of a list of list
of strings. The following is an example transaction command, for example

    [
        ["add", "23jy6", "README"],
        ["note", "Reformat text files"]
    ]

## Transaction Command Language

A *source path* and a *target path* are paths to an entry inside a given item.
They use forward slashes to distinguish between levels. Paths beginning with a
forward slash, have the initial slash removed. Example paths are

    README.md
    metadata/item-123.n3
    a/deep/path/to/a/file
    /pdf/pdf-1

In the last case, the path is normalized to be `pdf/pdf-1`.

An *extended source path* is either a source path, or it is a path having one
of the following forms

    @vvv/source/path
    @blob/nnnn

Where `@vvv` is a version identifier for the current item, and `nnnn` is a blob
identifier for the current item. Examples extended source paths are

    @5/README.md
    @12/a/deep/path/to/a/file
    @blob/45
    @blob/1

An *upload id* is a short string containing lowercase letters and numerals.
Examples:

    iaq7ye
    34gh90


### add

Add will take a source and assign it to a file entry in the item. The source
may be either a previously uploaded file or a blob currently in the item. If
the file entry currently exists, it is updated. Otherwise the entry is created.

Form:

    add <upload id> <target path>

The source may either be an upload id or a blob. Upload ids are passed as-is.
Blobs are identified either as `@blob/nnn` where `nnn` is the blob id, or
as `@v/file/path` where `v` is 

### move

Move will rename a file entry inside the item.

    move <source path> <target path>

### copy

Copy will duplicate a file entry inside an item. It is similar to move except
the source entry is also kept.

    copy <extended source path> <target path>

### remove

Remove will remove a file entry, but not the underlying data. This way previous
versions of the item will still be complete.

    remove <target path>

Examples:

    ["remove", "src/README.md"]

This command will remove the entry `src/README.md` in the new version of this
item. But the file will still be recoverable from the previous version of the
item.

### delete

Delete will remove a file entry or a blob completely, and expunge it from the
underlying storage. The file entry itself is kept in place.

This command is expected to be used rarely. Between `delete` and `remove`,
prefer `remove`---its semantics are closer to the preservation spirit of Bendo.
The `delete` command is provided to handle an exceptional use case, that of
removing data which absolutely never should have been added to the repository
in the first place.

    delete <extended source path>

### note

Note will set the version ingest note to the given string. If there is more than
one note command, only the last one will be used.

    note <text>

### sleep

Sleep will pause the ingest process for 1 second. It is intended to be used
for testing Bendo. Example usage:

    sleep

