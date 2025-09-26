# S3 WAL (Write-Ahead Log)

A simple **Write-Ahead Log (WAL)** implementation using AWS S3 as the storage backend, written in Go. Each log record is stored as a separate object in S3, enabling durable, append-only logging with recovery, reading, and truncation capabilities.

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

## Requirements

Go 1.21+

AWS account with an S3 bucket

AWS credentials configured via environment variables, AWS CLI, or .env file

## S3 Object Format


Example key: `wal/00000000000000000001`

---

## Installation

```bash
git clone https://github.com/roshr2211/s3-wal.git
cd s3-wal
go mod tidy

# Build the CLI tool
go build -o s3wal ./cmd/s3wal

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
 go run main1.go
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

Or you can pass the bucket and prefix via CLI flags:
```
--bucket <bucket-name> --prefix <prefix>
```

## CLI Usage
```
# Recover WAL state from S3
./s3wal --bucket your-bucket-name --prefix wal-demo recover

# Append records
./s3wal --bucket  your-bucket-name --prefix wal-demo append "Record #1"
./s3wal --bucket  your-bucket-name --prefix wal-demo append "Record #2"

# Read records
./s3wal --bucket  your-bucket-name --prefix wal-demo read 1
./s3wal --bucket  your-bucket-name --prefix wal-demo read 2

# Get the last record
./s3wal --bucket  your-bucket-name --prefix wal-demo last

# Truncate WAL after a specific offset
./s3wal --bucket  your-bucket-name --prefix wal-demo truncate 2
```


## How It Works

Each WAL record is stored as a separate S3 object:

```
<prefix>/<20-digit zero-padded offset>
```


Record structure:
```
[8-byte offset][data][32-byte SHA256 checksum]
```

On startup or before any operation, Recover scans S3 to find the latest offset.

Appending a new record automatically increments the offset.

Truncate deletes all records after a given offset.

## Example Output
```
$ ./s3wal --bucket  your-bucket-name --prefix wal-demo append "Hello World"
Appended record at offset: 1, data: Hello World

$ ./s3wal --bucket  your-bucket-name --prefix wal-demo read 1
Offset: 1, Data: Hello World

$ ./s3wal --bucket  your-bucket-name --prefix wal-demo last
Last record: offset=1, data=Hello World

$ ./s3wal --bucket  your-bucket-name --prefix wal-demo truncate 0
Truncated WAL after offset 0
```

- A script is added for CLI testing

```
chmod +x test_wal.sh
./test_wal.sh

```

