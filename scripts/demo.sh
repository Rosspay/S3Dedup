#!/bin/sh
set -eu

config=/etc/s3-dedup/demo.yaml
first_report=/demo/report-first.json
second_report=/demo/report-second.json

rm -f \
    /demo/state.db \
    /demo/state.db-shm \
    /demo/state.db-wal \
    /demo/report.json \
    "$first_report" \
    "$second_report" \
    /demo/s3-dedup.log

mc alias set demo http://minio:9000 demo-access-key demo-secret-key
mc mb --ignore-existing demo/s3-dedup-demo
mc rm --recursive --force demo/s3-dedup-demo
mc mirror --overwrite /control demo/s3-dedup-demo/input

assert_field() {
    report=$1
    field=$2
    expected=$3
    actual=$(jq -r ".$field" "$report")

    if [ "$actual" != "$expected" ]; then
        echo "$report: $field=$actual, expected $expected" >&2
        exit 1
    fi
}

run_scan() {
    /usr/local/bin/s3-dedup scan-once --config "$config"
}

run_scan
mv /demo/report.json "$first_report"

assert_field "$first_report" mode pointer
assert_field "$first_report" objects_scanned 120
assert_field "$first_report" unique_blobs 6
assert_field "$first_report" duplicates_found 114
assert_field "$first_report" objects_relinked 120
assert_field "$first_report" errors 0

run_scan
mv /demo/report.json "$second_report"

assert_field "$second_report" mode pointer
assert_field "$second_report" objects_scanned 120
assert_field "$second_report" unique_blobs 6
assert_field "$second_report" duplicates_found 114
assert_field "$second_report" objects_relinked 0
assert_field "$second_report" errors 0

if [ ! -s /demo/s3-dedup.log ]; then
    echo "demo log file is empty" >&2
    exit 1
fi

echo "Demo passed: first scan deduplicated 120 objects into 6 blobs."
echo "Demo passed: second scan made no S3 changes."
echo "Reports: build/demo/report-first.json and build/demo/report-second.json"
