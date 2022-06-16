S3 bulk copy object
===================

With this tool, you can directly copy objects from one S3 bucket to another in the same AWS Region without downloading the object..

Install
-------

```
go install github.com/n0madic/s3-bulk-copy-object@latest
```

Help
----

```
Usage: s3-bulk-copy-object [--acl ACL] [--concurrency NUM] [--recursive] [--region REGION] [--storage-class CLASS] [--timeout SECONDS] [--wait] SOURCE DESTINATION

Positional arguments:
  SOURCE                 Source bucket
  DESTINATION            Destination bucket

Options:
  --acl ACL, -a ACL      ACL to apply to the copied object
  --concurrency NUM, -c NUM
                         Number of concurrent transfers [default: 10]
  --recursive, -r        Recursively copy all objects in the source bucket
  --region REGION        AWS region [default: us-east-1]
  --storage-class CLASS
                         Storage class to apply to the copied object [default: STANDARD]
  --timeout SECONDS, -t SECONDS
                         Copy timeout in seconds [default: 60]
  --wait, -w             Wait for the item to be copied
  --help, -h             display this help and exit
```

Usage
-----

```
s3-bulk-copy-object --region us-west-1 --recursive s3://bucket1/ s3://bucket2/backup/
```
