"""S3 persistence helpers for OneClick analysis artifacts."""

from __future__ import annotations

import os
from pathlib import Path

import boto3


ONECLICK_S3_BUCKET = "oneclick-s3-analysis" #os.environ.get("ONECLICK_S3_BUCKET", "").strip()
ONECLICK_S3_ANALYSIS = (
	f"s3://{ONECLICK_S3_BUCKET}"
	if ONECLICK_S3_BUCKET
	else ""
)

def _split_s3_uri(s3_uri: str) -> tuple[str, str]:
	if not s3_uri.startswith("s3://"):
		raise ValueError(f"Invalid S3 URI: {s3_uri}")

	bucket_and_key = s3_uri[5:]
	bucket, _, key_prefix = bucket_and_key.partition("/")
	return bucket, key_prefix.rstrip("/")


def save_json_file_to_s3(json_file_name: str = "test.json", record_id: str = "REC") -> dict:
	"""Create a JSON file directly in the configured S3 location.

	Args:
		json_file_name: Path to the local JSON file to upload.

	Returns:
		Metadata describing the uploaded object location.
	"""
	if not ONECLICK_S3_BUCKET or not ONECLICK_S3_ANALYSIS:
		raise EnvironmentError("ONECLICK_S3_BUCKET environment variable is not set")

	file_name = Path(json_file_name).name

	if Path(file_name).suffix.lower() != ".json":
		raise ValueError(f"Expected a JSON file, got: {json_file_name}")

	record_id_value = record_id.strip()
	if not record_id_value:
		raise ValueError("record_id is required")

	bucket, key_prefix = _split_s3_uri(ONECLICK_S3_ANALYSIS)
	object_key = (
		f"{key_prefix}/{record_id_value}/{file_name}"
		if key_prefix
		else f"{record_id_value}/{file_name}"
	)
	s3_uri = f"s3://{bucket}/{object_key}"

	print(s3_uri, bucket, object_key)
	
	s3_clinet = boto3.client(
		"s3",
		aws_access_key_id="",
		aws_secret_access_key=""
    )
		
	s3_clinet.put_object(
		Bucket=bucket,
		Key=object_key,
		Body='{"x": "test data"}',
		ContentType="application/json",
	)


if __name__ == "__main__":
    save_json_file_to_s3()