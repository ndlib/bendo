/*

Bendo provides routines to manulipate and serialize items.
An item is defined as a collection of blobs and versions.
Blobs are immutable binary blocks of data. Blobs may be added to an item over
time, and may be deleted, but they cannot be otherwise altered once added.
Versions provide a way to associate labels to blobs, and may be added to an
item over time, but cannot be deleted.

Items are serialized into a sequence of bundles. Each bundle file is immutable.
However, like blobs, bundle files may be deleted.

Items do not share blobs between them. Bundle files do not contain information
from more than one item.

A Store provides the logic to do the serialization and deserialization of items
to bundles. It wraps a BundleStore interface. It will block. It is possible to
add a cache to store item metadata. The data retreival paths of a store are
safe to be accessed from multiple goroutines. However, an open Writer for any
given item should only be used by one gouroutine at a time.

Both blobs and versions are numbered sequentually starting from 1.

An item's metadata and blobs are grouped into bundles, which are zip files.
Each bundle contains the complete up-to-date metadata information on an item,
as well as zero or more blobs. Bundles are numbered, but they should not be
assumed to be numbered sequentially since deletions may remove some bundles.
Bundle numbers for an item start from 1. The largest numbered bundle must
contain the most up-to-date information on the item, including the (correct!)
blob to bundle mapping.

There is no relationship between a bundle number and the versions of an item.

*/
package items
