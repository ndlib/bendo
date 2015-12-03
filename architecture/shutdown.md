
# Bendo shutdown sequence

Since there may be long-running background processing happening, it is
advisable to not simply stop the server. Instead, send the server a signal
(TODO: which signal? SIGUSR2?) and it will begin a two phase shutdown process.

## Phase 1

Phase 1 will suspend all queued item update transactions which are waiting to
run, and then wait for the currently running transactions to finish. No
transactions may be started during this time---starting a transaction will save
it to the pending transaction list, but the processing will not begin. When all
transactions are finished, phase 2 starts.

This phase has the potential to last a long time, especially if the currently
running transactions are large.

## Phase 2

Phase 2 will have the server stop accepting HTTP requests. This may take some
time since a file upload has a potentially unbounded execution time. However,
our clients send files in smallish pieces (up to 100 MB), so phase 2 is not
expected to last more than a few seconds. At the end of phase 2, the server
will exit.

