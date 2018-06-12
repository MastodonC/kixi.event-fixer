# kixi.event-fixer

A utilty library to transfer our event log from the broken nippy files to baldr wrapped nippy files

## Usage

This is a repl driven tool, that only had a one time use, hence it being a bit messy. The event_fixer namespace is the main entry point, with the download-s3-backups-and-transform function that runs the whole fix. The other fns in there are either helpers for that process or checkers for the files. The other namespaces just do what they say on the tin.

The customised Nippy version contained within shouldn't be used for anything else, it has been modified to correct the specific problems we had with our Events.

---
## Steps to construct new history:

https://witanblog.wordpress.com/2018/02/15/34515/

### Create new bucket and adjust lambdas to point at new bucket

``` shell
cd terraboot-witan
git pull
emacs resources/prod.edn
```

* Adjust `:s3-bucket-control-args -> :bucket-names` to include the **old** bucket name. (Found in `:common -> :event-bucket`)
* Adjust `:s3-bucket-control-args -> :policies` to include a new policy for the **old** bucket name.
* Adjust `:dcos-cluster-infra-args -> :slave-policies -> :name "dcos-slave-s3-readonly-access" -> :resources` to include the **old** bucket name.
* Adjust `:common -> :event-bucket` to be the **new** bucket name.

``` shell
mach plan
```

**There should be NO DELETIONS (red text) listed in the plan output. If you see red text, DO NOT CONTINUE.**
You should see 5 updates (yellow text) and 2 creates (green text).

Create a new branch with your changes, open a PR and have some one review. Include the changes from terraform plan output in the PR description. ([Example](https://github.com/MastodonC/terraboot-witan/pull/155))

Once approved, merge and checkout master.

``` shell
mach plan
```

Check the output one more time.

``` shell
mach apply
```

(If it fails to apply bucket policies due to ordering ensure you `mach plan` before you `mach apply` again)

Make a note of the hour.

At this point it's worth eye-balling to confirm new events are appearing in the new bucket.

### Create transformation

``` shell
cd kixi.event-fixer
cp src/kixi/event_log_XXXX_to_event_log_XXXX.clj src/kixi/event_log_XXXX_to_event_log_YYYY.clj # pick a recent transformation to copy
emacs src/kixi/event_log_XXXX_to_event_log_YYYY.clj
```

* Adjust `s3-source-bucket` to the **old** bucket name
* Adjust `s3-destination-bucket` to the **new** bucket name
* Adjust `backup-end-hour` to an hour *past* the hour you recorded earlier (e.g. `14:56` => `15`)

Make sure both the source and destination directories inside `/event-log` are created.

In the source code identify the function you need to change (might be `fix-bad-event`). This is the function that will iterate over each event as it's downloaded. This is where you should start with your transformations.

* Set the `AWS_REGION` environment variable to the region of the new bucket (e.g. "eu-west-1")
(TODO: Perhaps `:endpoint` key in the credentials?)
* Start a REPL and navigate to the namespace of the new source code.
* Call `download-s3-backups-and-transform`

### Upload your fixed event-log

* Navigate to the `kixi.uploader` namespace
* Adjust `target-bucket` to the **new** bucket name
* Adjust `credentials` to use a new MFA key
* Call `upload-files`

Once complete, eye-ball the changes in the AWS console.

You're done!
