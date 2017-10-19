# kixi.event-fixer

A utilty library to transfer our event log from the broken nippy files to baldr wrapped nippy files

## Usage

This is a repl driven tool, that only had a one time use, hence it being a bit messy. The event_fixer namespace is the main entry point, with the download-s3-backups-and-transform function that runs the whole fix. The other fns in there are either helpers for that process or checkers for the files. The other namespaces just do what they say on the tin.

The customised Nippy version contained within shouldn't be used for anything else, it has been modified to correct the specific problems we had with our Events.
