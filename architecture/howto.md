# HOW-TO

This document lists some how-tos, both for doing things to a server and for running a server.

## How to get a list of every item in a Bendo server

This uses the _bundle_ APIs to get a list of every bundle file stored. Since an item may have more than one bundle file,
we consolidate the bundle names to get a list of item names.
Replace `$apikey` with your bendo key and `$bendo` with the url to your bendo server.

```bash
curl -u ":$apikey" "$bendo:14000/bundle/list/" | jq -r 'map(.|sub("-.*$";""))|unique|.[]' > bendo-inventory
```

## How to get metadata for a single item

You can view the information for a single item as html by visiting the URL `$bendo:14000/item/$itemid`

You can get the metadata formatted as JSON as well.

```
curl -u ":$apikey" "$bendo:14000/item/$itemid?format=json"
```

## How to get metadata for a list of items

This puts the previous answer into a loop. It is assumed the file `bendo-inventory` contains a list of bendo IDs, with ID one per line.

```
for itemid in $(cat bendo-inventory); do
    curl -s -u ":$apikey" "$bendo:14000/item/$itemid?format=json"
done > bendo-items.json
```
