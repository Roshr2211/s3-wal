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
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	bucket := os.Getenv("AWS_BUCKET_NAME")
	prefix := os.Getenv("AWS_PREFIX")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(os.Getenv("AWS_REGION")),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)
	wal := s3_log.NewS3WAL(client, bucket, prefix)

	ctx := context.TODO()

	// Recover WAL state
	lastOffset, _ := wal.Recover(ctx)
	fmt.Println("Last offset:", lastOffset)

	// Append multiple records
	for i := 1; i <= 5; i++ {
		data := []byte(fmt.Sprintf("Record #%d", i))
		offset, err := wal.Append(ctx, data)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		fmt.Printf("Appended record at offset: %d, data: %s\n", offset, string(data))
	}

	// Read specific record
	readOffset := uint64(3)
	rec, err := wal.Read(ctx, readOffset)
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	fmt.Printf("Read record at offset %d: %s\n", rec.Offset, string(rec.Data))

	// Get last record
	lastRec, err := wal.LastRecord(ctx)
	if err != nil {
		log.Fatalf("LastRecord failed: %v", err)
	}
	fmt.Printf("Last record: offset=%d, data=%s\n", lastRec.Offset, string(lastRec.Data))

	// Truncate after offset 2
	fmt.Println("Truncating WAL after offset 2...")
	if err := wal.Truncate(ctx, 2); err != nil {
		log.Fatalf("Truncate failed: %v", err)
	}

	// Recover again
	lastOffset, _ = wal.Recover(ctx)
	fmt.Println("Last offset after truncate:", lastOffset)
}
