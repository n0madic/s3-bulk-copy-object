package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var args struct {
	Source       string `arg:"positional,required" help:"Source bucket"`
	Destination  string `arg:"positional,required" help:"Destination bucket"`
	ACL          string `arg:"-a,--acl" help:"ACL to apply to the copied object"`
	Concurrency  int    `arg:"-c,--concurrency" placeholder:"NUM" help:"Number of concurrent transfers" default:"10"`
	Recursive    bool   `arg:"-r,--recursive" help:"Recursively copy all objects in the source bucket"`
	Region       string `arg:"--region" help:"AWS region" default:"us-east-1"`
	StorageClass string `arg:"--storage-class" placeholder:"CLASS" help:"Storage class to apply to the copied object" default:"STANDARD"`
	Timeout      int    `arg:"-t,--timeout" placeholder:"SECONDS" help:"Copy timeout in seconds" default:"60"`
	Wait         bool   `arg:"-w,--wait" help:"Wait for the item to be copied"`
}

func main() {
	arg.MustParse(&args)
	logerr := log.New(os.Stderr, "", 0)
	loginfo := log.New(os.Stdout, "", 0)

	source, err := url.Parse(args.Source)
	if err != nil {
		logerr.Printf(err.Error())
		os.Exit(1)
	}
	target, err := url.Parse(args.Destination)
	if err != nil {
		logerr.Printf(err.Error())
		os.Exit(2)
	}
	if source.Scheme != "s3" || target.Scheme != "s3" {
		logerr.Println("Source and target must be s3:// urls")
		os.Exit(3)
	}

	// Initialize a session in that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args.Region)},
	)
	if err != nil {
		logerr.Printf("Failed to create AWS session: %v\n", err)
		os.Exit(4)
	}

	// Create S3 service client
	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if args.Timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, time.Duration(args.Timeout)*time.Second)
	}
	// Ensure the context is canceled to prevent leaking.
	if cancelFn != nil {
		defer cancelFn()
	}

	semaphore := make(chan struct{}, args.Concurrency)
	var wg sync.WaitGroup

	// Object copy function.
	copyObject := func(sourceBucket, sourcePath, targetBucket, targetPath string) {
		if args.Recursive {
			defer func() { <-semaphore }()
			defer wg.Done()
		}
		// Copy the item from the source bucket to the destination bucket.
		_, err := svc.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
			CopySource:   aws.String(url.QueryEscape(path.Join(sourceBucket, sourcePath))),
			Bucket:       aws.String(targetBucket),
			Key:          aws.String(targetPath),
			ACL:          aws.String(args.ACL),
			StorageClass: aws.String(args.StorageClass),
		})
		if err != nil {
			logerr.Printf("Failed to copy object %s: %v\n", sourcePath, err)
			return
		}
		// Wait for the item to be copied
		if args.Wait {
			err = svc.WaitUntilObjectExistsWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(targetBucket),
				Key:    aws.String(targetPath),
			})
			if err != nil {
				logerr.Printf("Failed to wait for object %s: %v\n", targetPath, err)
				return
			}
		}
		loginfo.Printf("Item %q successfully copied from bucket %q to bucket %q\n", sourcePath, sourceBucket, targetBucket)
	}

	if args.Recursive {
		// List all objects in the source bucket and copy them to the target bucket
		err = svc.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(source.Host),
			Prefix: aws.String(strings.TrimPrefix(source.Path, "/")),
		}, func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.Contents {
				sourcePath := aws.StringValue(o.Key)
				targetPath := path.Join(target.Path, sourcePath)
				wg.Add(1)
				semaphore <- struct{}{}
				go copyObject(source.Host, sourcePath, target.Host, targetPath)
			}
			return true // continue paging
		})
		if err != nil {
			logerr.Printf("Failed to list objects for source bucket %s: %v\n", source.Host, err)
			os.Exit(5)
		}
		wg.Wait()
	} else {
		// Copy onces the item to the target bucket
		targetPath := path.Join(target.Path, source.Path)
		copyObject(source.Host, source.Path, target.Host, targetPath)
	}
}
