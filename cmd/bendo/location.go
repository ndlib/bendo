package main

import (
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/networking"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/ndlib/bendo/store"
)

// splitBucketPrefix will take a path and separate the bucket name from a prefix, if any.
// It will also append "addtion" to the prefix, and make sure the prefix returned is
// either empty or ends with a slash "/".
//
// examples:
// 		"" -> ("", "")
//		"bucket" -> ("bucket", "")
//		"bucket/and/a/prefix" -> ("bucket", "and/a/prefix/")
func splitBucketPrefix(location string, addition string) (bucket, prefix string) {
	if location == "" {
		return
	}
	location = strings.TrimPrefix(location, "/")
	v := strings.SplitN(location, "/", 2)
	bucket = v[0]
	if len(v) > 1 {
		prefix = v[1]
	}
	if addition != "" {
		prefix = path.Join(prefix, addition)
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return
}

// parselocation will create an approprate store based on "location".
// In case of an error, nil is returned.
// If location is empty, a memory store is returned.
// It understands special schemes "s3:" and "blackpearl:".
// Use "blackpearls:" for a https connection to a BlackPearl device.
func parselocation(location string, addition string) store.Store {
	if location == "" {
		return store.NewMemory()
	}
	u, _ := url.Parse(location)
	switch u.Scheme {
	case "", "file":
		path := filepath.Join(u.Path, addition)
		os.MkdirAll(path, 0755)
		return store.NewFileSystem(path)
	case "s3":
		conf := &aws.Config{}
		if u.Host != "" {
			conf.Endpoint = aws.String(u.Host)
			conf.Region = aws.String("us-east-1")
			// disable SSL for local development
			if strings.Contains(u.Host, "localhost") {
				conf.DisableSSL = aws.Bool(true)
				conf.S3ForcePathStyle = aws.Bool(true)
			}
		}
		bucket, prefix := splitBucketPrefix(u.Path, addition)
		if bucket == "" {
			log.Println("Error parsing location, no bucket name", location)
			return nil
		}
		return store.NewS3(bucket, prefix, session.New(conf))
	case "blackpearl", "blackpearls":
		tempdir := os.Getenv("DS3_TEMPDIR") // okay if returns ""
		accessKey := os.Getenv("DS3_ACCESS_KEY")
		secretKey := os.Getenv("DS3_SECRET_KEY")
		switch {
		case accessKey == "":
			log.Fatalln("DS3_ACCESS_KEY missing")
		case secretKey == "":
			log.Fatalln("DS3_SECRET_KEY missing")
		}
		// build the URL for the blackpearl
		endpoint := &url.URL{
			Scheme: "http",
			Host:   u.Host,
		}
		if u.Host == "blackpearls" {
			endpoint.Scheme = "https"
		}
		bucket, prefix := splitBucketPrefix(u.Path, addition)
		if bucket == "" {
			log.Println("Error parsing location, no bucket name", location)
			return nil
		}
		bp := ds3.NewClientBuilder(
			endpoint,
			&networking.Credentials{AccessId: accessKey, Key: secretKey},
		).BuildClient()
		s := store.NewBlackPearl(bucket, prefix, bp)
		s.TempDir = tempdir
		return s
	}
	// there was some kind of error. Return a Memory store? or fail?
	log.Println("Problem parsing location", location)
	return nil
}
