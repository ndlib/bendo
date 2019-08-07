# Command lines for `butil`, the bendo command line utility

This command provides several utility functions for working with a bendo
storage tree. This utility is designed to run directly on a storage filesystem.
Use `bclient` to interact with a bendo server.

# Usage

    butil [options] <command> <command arguments>

Options:

  -storage <path>
        The root of the storage tree. Defaults to `.`.

  -creator <name>
        The name of the creator to use, if needed. Defaults to `butil`.

  -verbose
        Flag to increase the amount of logging output.


## Commands

### blob

    blob <item id> <blob number>

Retreives a specific blob from a given item. The blob's contents are sent to
STDOUT.

example:

    butil blob abcd1234 56


### item

    item <item id list>

Prints a human-readable output describing each item passed in. It will print
information about the item overall, an entry for each version and information
for each blob.


### list

    list

List will print to STDOUT an item id for each item in the storage area.

example:

    butil list


### add

    add <item id> <file/directory list>

Add will add each file and directory to the given item. Files and directories
that begin with a dot are skipped.


### set

    set <item id> <file/directory list>

Set is like add, except it also clears any existing slot entries, so only the
given files are mapped in the newest version.


### delete

    delete <item id> <blob number list>

Delete will delete the given blobs from the underlying item. It does this by
copying any other blobs in the same bundles as the target deleted blobs into a
new bundle. Then it removes the bundles that contain the target blobs.


### identify-missing-blobs

    identify-missing-blobs

This will scan the storage looking for items that have unmapped blobs. (This is
a rare error condition of the server.)


### fix-missing-blobs

    fix-missing-blobs <item id list>

This will fix specific items as identified by `identify-missing-blobs`. If a
passed in id does not have the error condition, it is ignored.
