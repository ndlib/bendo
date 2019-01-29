Changes to Bendo and its associated commands and packages. Ordered from newest
to oldest.

* 2016.3 (2016-11-17)

 ** bendo server

 - Add ability to disable access to tape system (for when tape system is
 down for maintenance or is otherwise unavailable. It can be toggled using
 the `/admin/use_tape` route.
 - Change MySQL fields to use LONGTEXT to store item JSON, and BIGINT to store
 an item's total size in bytes. Also add index on the item ids, and change
 query to make sure an item is not cached more than once in the database.
 - Add parameter to supply a token to the initialization of the Copy-on-Write
 feature. Change the COW subsystem to group together requests to download the
 same bundle file from the remote server.

 ** bclient

 - Let client overwrite download files

 ** Misc

 - Move from `Godeps/_workspace` to `vendor`


* 2016.2 (2016-6-15)

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
 - Add X-Cache == 2 header for content which is too large to be cached

 ** bclient

 - Add `-chunksize` command line option
 - Add `--wait` option
 - Exclude directories beginning with a `.`

 - Fixed issues: #10, #17, #36, #59, #60, #63, #69, #70, #70, #74, #77, #83

* 2016.1 (2016-2-2)

 - Initial Release

* 2018.1
 - Only return fixity checks from current day
 - Change `start` and `end` of Fixity Search queries to default to midnight
    of the current day, and of the next day.
    This (hopefully, for now) keeps too many results from being returned.
 -  Return [] for empty fixity search results
 -  Let bclient upload zero length files
 -  Aws codepipeline
 - swap .gitignore and .dockerignore
 - added public bucket policy
 - update CodeBuildRole parameter
 - use hesburghlibraries/bendo-buildimage for codebuild
 =  add Dockerfile.buildimage
 - Rpm build using codebuild
 - upgrade timeout and compute type
 - fix sha check
 - wget -> curl

* 2019.2 (1/29/19)

 -  gofmt depenedencies
 -  Add hooks for Sentry
 -  Update all deps
 -  Revise readme
 -  Add config options for S3 to bendo daemon
 -  Add config option to use the time-based cache evection strategy.
 -  Always write tim-based cache index file after a scan. This ensures
    items get timestamped in case the server is restarted before the next
    index save.
 -  Adding S3 store adapter
 -  Altered the Cache interface slightly. Added MaxSize().
 -  Add option to put token in basic auth header
 
