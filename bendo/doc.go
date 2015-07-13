/*

Each item consists of a list of blobs and a list of versions. Each blob contains
binary content. Each version maps symbolic names to specific blobs. Items do not
share blobs or versions with other items.

Blobs are numbered from 1. The blob with id 0 has the special meaning of the
empty length stream.


An item's metadata and blobs are grouped into "bundles", which are zip files.
Each bundle contains the complete up-to-date metadata information on an item,
as well as zero or more blobs. Bundles are numbered, but they should not be
assumed to be numbered sequentially since deletions may remove some bundles.
Bundle numbers for an item start from 1. The largest numbered bundle must
contain the most up-to-date information on the item, including the (correct!)
blob to bundle mapping.

There is no relationship between a bundle number and the versions of an item.

*/
package bendo
