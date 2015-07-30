/*
Transaction handles the details of storing and manulipulating the transactions
for bendo. These are the transactions from the viewpoint of the web UI. Eventually
these transactions are turned into a sequence of calls to an item.Writer object.

This package builds on the fragment package. A transaction is represented by a
bunch of Files; one for the transaction metadata, and as many more as are needed
to hold the blobs to be added to the given item.
*/
package transaction
