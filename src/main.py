import os
from .celery_task import load_one_file_to_snowflake


if __name__ == "__main__":
    root_dir = "/Users/baothainguyengia/Library/Mobile Documents/com~apple~CloudDocs/Desktop/Project/[btl]DBMS/01_MRI_Data"

    count = 0

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.lower().endswith(".ima"):
                load_one_file_to_snowflake(os.path.join(dirpath, fname))
                count += 1
                if count % 100 == 0:
                    print(f"Dispatched {count} tasks...")
