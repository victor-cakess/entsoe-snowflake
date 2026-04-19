import os
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET")
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")

s3 = boto3.client(
    "s3",
    config=Config(max_pool_connections=50)
)

transfer_config = TransferConfig(
    max_concurrency=50,
    use_threads=True,
)


def collect_files():
    files = []
    for root, _, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            if filename.endswith(".parquet"):
                local_path = os.path.join(root, filename)
                s3_key = os.path.relpath(local_path, BASE_DIR)
                files.append((local_path, s3_key))
    return files


def upload_file(local_path, s3_key):
    s3.upload_file(local_path, BUCKET, s3_key, Config=transfer_config)
    return s3_key


def main():
    files = collect_files()
    print(f"Found {len(files)} parquet files to upload")

    failed = []
    with tqdm(total=len(files), unit="file", ncols=80) as pbar:
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {
                executor.submit(upload_file, local_path, s3_key): s3_key
                for local_path, s3_key in files
            }
            for future in as_completed(futures):
                s3_key = futures[future]
                try:
                    future.result()
                except Exception as e:
                    failed.append((s3_key, str(e)))
                pbar.update(1)

    print(f"\nUpload complete — {len(files) - len(failed)} succeeded, {len(failed)} failed")
    if failed:
        print("\nFailed files:")
        for s3_key, error in failed:
            print(f"  {s3_key}: {error}")


if __name__ == "__main__":
    main()