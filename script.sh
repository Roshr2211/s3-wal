#!/bin/bash
# test_wal.sh
# A test script to verify S3 WAL operations

BUCKET="your-bucket-name"  # Replace with your S3 bucket name
PREFIX="wal-demo"

echo "=== Recover WAL state ==="
./s3wal --bucket "$BUCKET" --prefix "$PREFIX" recover
echo

echo "=== Appending records ==="
for i in {1..5}; do
    DATA="Record #$i"
    ./s3wal --bucket "$BUCKET" --prefix "$PREFIX" append "$DATA"
done
echo

echo "=== Reading records ==="
for i in {1..5}; do
    ./s3wal --bucket "$BUCKET" --prefix "$PREFIX" read $i
done
echo

echo "=== Reading last record ==="
./s3wal --bucket "$BUCKET" --prefix "$PREFIX" last
echo

echo "=== Truncating WAL after offset 3 ==="
./s3wal --bucket "$BUCKET" --prefix "$PREFIX" truncate 3
echo

echo "=== Recovering WAL after truncate ==="
./s3wal --bucket "$BUCKET" --prefix "$PREFIX" recover
echo

echo "=== Attempting to read deleted records (offset 4 and 5) ==="
for i in 4 5; do
    ./s3wal --bucket "$BUCKET" --prefix "$PREFIX" read $i
done

echo
echo "=== WAL Test Completed ==="
