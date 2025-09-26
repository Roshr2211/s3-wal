# S3 WAL (Write-Ahead Log)

A Go implementation of a Write-Ahead Log (WAL) that persists each log record as an object in **AWS S3**. Each record includes an offset, data, and SHA256 checksum for integrity.

---

## Features

- **Append** records to S3 with incremental offsets.
- **Read** records from S3 at a specific offset.
- **LastRecord** retrieves the latest log record.
- **Truncate** deletes records after a specified offset.
- **Recover** initializes WAL state from existing S3 objects.
- **Data integrity** via SHA256 checksums.
- **Concurrency-safe** using mutexes.

---

## S3 Object Format


Example key: `wal/00000000000000000001`

---

## Installation

```bash
git clone <repo-url>
cd s3-wal-demo
go mod tidy
```

## Configuration
Create a .env file with:
```
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_REGION=us-east-1
AWS_BUCKET_NAME=<your-bucket>
AWS_PREFIX=wal
```
 - Make sure your S3 bucket exists and your IAM user has full S3 access.

 ## Usage
 ```bash 
 go run main.go
 ```

## example output
```
Last offset: 0
Appended record at offset: 1, data: Record #1
Appended record at offset: 2, data: Record #2
Read record at offset 2: Record #2
Last record: offset=2, data=Record #2
Truncating WAL after offset 1...
Last offset after truncate: 1
```