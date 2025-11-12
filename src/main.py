import os
from .celery_task import load_one_file_to_snowflake




if __name__ == "__main__":
    root_dir = "/Users/baothainguyengia/Desktop/[btl]DBMS/01_MRI_Data"

    sf_config = dict(
        sf_user="tngiabao",
        sf_password="Giabao@22102652210265",
        sf_account="iw82368.ap-southeast-1",
        sf_warehouse="COMPUTE_WH",
        sf_database="DBMS",
        sf_schema="PUBLIC",
    )
    count = 0
    
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.lower().endswith(".ima"):
                load_one_file_to_snowflake.delay(os.path.join(dirpath, fname))
                count += 1
                if count % 100 == 0:
                    print(f"Dispatched {count} tasks...")
                
