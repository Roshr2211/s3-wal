package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"s3-wal-demo/s3_log"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// CLI flags
	awsRegion := flag.String("region", os.Getenv("AWS_REGION"), "AWS region")
	bucket := flag.String("bucket", os.Getenv("AWS_BUCKET_NAME"), "S3 bucket name")
	prefix := flag.String("prefix", os.Getenv("AWS_PREFIX"), "S3 prefix for WAL")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Println("Usage: s3wal <command> [args]")
		fmt.Println("Commands: append <data>, read <offset>, last, truncate <offset>, recover")
		return
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*awsRegion))
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)
	wal := s3_log.NewS3WAL(client, *bucket, *prefix)
	ctx := context.TODO()

	cmd := flag.Arg(0)
	switch cmd {
	case "append":
		if len(flag.Args()) < 2 {
			log.Fatal("Usage: s3wal append <data>")
		}
		data := []byte(flag.Arg(1))
		offset, err := wal.Append(ctx, data)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		fmt.Printf("Appended record at offset: %d, data: %s\n", offset, string(data))

	case "read":
		if len(flag.Args()) < 2 {
			log.Fatal("Usage: s3wal read <offset>")
		}
		offset, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			log.Fatalf("Invalid offset: %v", err)
		}
		rec, err := wal.Read(ctx, offset)
		if err != nil {
			log.Fatalf("Read failed: %v", err)
		}
		fmt.Printf("Offset: %d, Data: %s\n", rec.Offset, string(rec.Data))

	case "last":
		rec, err := wal.LastRecord(ctx)
		if err != nil {
			log.Fatalf("LastRecord failed: %v", err)
		}
		fmt.Printf("Last record: offset=%d, data=%s\n", rec.Offset, string(rec.Data))

	case "truncate":
		if len(flag.Args()) < 2 {
			log.Fatal("Usage: s3wal truncate <offset>")
		}
		offset, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			log.Fatalf("Invalid offset: %v", err)
		}
		if err := wal.Truncate(ctx, offset); err != nil {
			log.Fatalf("Truncate failed: %v", err)
		}
		fmt.Printf("Truncated WAL after offset %d\n", offset)

	case "recover":
		lastOffset, err := wal.Recover(ctx)
		if err != nil {
			log.Fatalf("Recover failed: %v", err)
		}
		fmt.Printf("Last offset: %d\n", lastOffset)

	default:
		fmt.Println("Unknown command:", cmd)
		fmt.Println("Commands: append <data>, read <offset>, last, truncate <offset>, recover")
	}
}
