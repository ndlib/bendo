/*
Package transaction contains higher level logic used for ingesting content.
It is intended to support the server package.
A transaction here is the same as a transaction from the viewpoint of the web UI.
Transactions handle the details of storing and manipulating the item uploads for
Bendo. Eventually these transactions are turned into a sequence of calls to an
item.Writer object.

This package builds on the fragment package. A transaction is represented by a
bunch of Files; one for the transaction metadata, and as many more as are needed
to hold the blobs to be added to the given item.

There is not really a clear distinction between what belongs in here verses
what belongs in the server. For now, all the goroutine code is in the server,
and the status monitoring straddles the two. Probably the status code should
be moved completely into one package or the other.
*/
package transaction
