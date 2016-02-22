Changes to Bendo and its associated commands and packages. Ordered from newest
to oldest.

* 2016.2 (2016-?)

 ** bendo server

 - Add `--cache-dir` command line option
 - Add `--mysql` command line option
 - Add `--copy-on-write` command line option and support for mirroring an
 external bendo server.
 - Graceful server shutdown upon receiving a SIGINT or SIGTERM.
 - Set bag tags giving the bendo identifier
 - Return content type of `application/json` where appropriate
 - Support the `/item/:id/@blob/nnn` syntax
 - Redirect `/item/:id/` to `/item/:id`. This case fell through the cracks
 since we use a splat route for item paths.

 ** bclient

 - Add `-chunksize` command line option
 - Add `--wait` option
 - Exclude directories beginning with a `.`

 - Fixed issues: #10, #17, #36, #59, #60, #63, #69, #70, #70, #74, #77, #83

* 2016.1 (2016-2-2)

 - Initial Release
