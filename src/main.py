import os
from .celery_task import load_one_file_to_snowflake
from .command.index import command_service
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 20

if __name__ == "__main__":
    root_dir = "/Users/baothainguyengia/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Project/[btl]DBMS/01_MRI_Data"

    ima_files = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.lower().endswith(".ima"):
                ima_files.append(os.path.join(dirpath, fname))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_path = {
            executor.submit(load_one_file_to_snowflake, p): p for p in ima_files
        }

        for i, future in enumerate(as_completed(future_to_path), start=1):
            # Raise exception immediately if any thread fails
            future.result()
            if i % 100 == 0:
                print(f"Processed {i} files...")

    print(f"Done. Total files processed: {len(ima_files)}")
