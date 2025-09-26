package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"s3-wal-demo/s3_log"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	bucket := os.Getenv("AWS_BUCKET_NAME")
	prefix := os.Getenv("AWS_PREFIX")
	awsRegion := os.Getenv("AWS_REGION")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)
	wal := s3_log.NewS3WAL(client, bucket, prefix)

	// Recover WAL state
	lastOffset, _ := wal.Recover(context.TODO())
	fmt.Println("Last offset:", lastOffset)

	// Append a record
	offset, err := wal.Append(context.TODO(), []byte("Hello S3 WAL!"))
	if err != nil {
		log.Fatalf("Append failed: %v", err)
	}
	fmt.Println("Appended record at offset:", offset)

	// Read last record
	lastRecord, err := wal.LastRecord(context.TODO())
	if err != nil {
		log.Fatalf("Failed to get last record: %v", err)
	}
	fmt.Printf("Last record: offset=%d, data=%s\n", lastRecord.Offset, string(lastRecord.Data))
}
