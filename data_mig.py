pip install pandas
pip install boto3
pip install sqlalchemy
pip install pymysql

import os
import zipfile
import requests

if not os.path.exists("downloaded_files"):
    os.makedirs("downloaded_files")

url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

zip_file_path = os.path.join("downloaded_files", "submissions.zip")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)


if response.status_code == 200:
    with open(zip_file_path, "wb") as f:
        f.write(response.content)
    print("Zip file downloaded successfully.")


    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall("downloaded_files")

    print("Zip file contents extracted.")
else:
    print("Failed to download the zip file.")
import boto3
import json
import pandas as pd
from sqlalchemy import create_engine
data_list=[]
s3_bucket_name = "vanathifinal"
s3_key = "submissions.json"  # The key under which the JSON data will be stored in S3


aws_access_key = "AKIAWK7KZTD6JH7WIOF6"
aws_secret_key = "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"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

data_json=json.dumps(data_list).encode('utf-8')


s3_client.put_object(
    Bucket=s3_bucket_name,
    Key=s3_key,
    Body=data_json,
    ContentType="application/json"
)

print("Data successfully stored in Amazon S3.")

rds_host = "database-company.cer7d9d8mbjl.ap-south-1.rds.amazonaws.com"
rds_port = "3306"
rds_user = "admin"
rds_password = "vanathi94"
rds_db_name = "database-company"


engine = create_engine(
    f"mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db_name}"
)

df = pd.DataFrame(data_list)
df.to_sql("data_table", engine, if_exists="replace", index=False)



print("Data migration and transformation completed successfully.")
