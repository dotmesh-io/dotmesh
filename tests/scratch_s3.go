package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

// file for testing out s3 bits

func main() {
	config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("alice", "password", ""),
		Endpoint:         aws.String("http://127.0.0.1:32607/s3"),
		S3ForcePathStyle: aws.Bool(true),
	}
	sess, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-east-1"))
	output, err := svc.PutObject(&s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
		Bucket: aws.String("alice-blue"),
		Key:    aws.String("hello-world"),
	})
	if err != nil {
		fmt.Printf("Err: %#v\n", err.Error())
	}
	fmt.Printf("Output: %#v", output)
}
