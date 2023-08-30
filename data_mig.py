pip install boto3
import os
import zipfile
import requests

if not os.path.exists("extracted_files"):
    os.makedirs("extracted_files")


url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

zip_file_path = os.path.join("extracted_files", "submissions.zip")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)


if response.status_code == 200:
    with open(zip_file_path, "wb") as f:
        f.write(response.content)
    print("Zip file downloaded successfully.")
else:
    print("Failed to download the zip file.")

extracted_files = []
#target_filenames = ["CIK0000000020.json"]
target_filenames = ["CIK0000000020.json", "CIK0000001761.json", "CIK0000001750.json", "CIK0000001800.json"]
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    for target_filename in target_filenames:
        if target_filename in zip_ref.namelist():
            extracted_file_path = os.path.join("extracted_files", target_filename)
            with open(extracted_file_path, "wb") as extracted_file:
                extracted_file.write(zip_ref.read(target_filename))
            extracted_files.append(extracted_file_path)

print("JSON files extracted:", extracted_files)
import boto3
import json

dynamodb = boto3.client('dynamodb', region_name='ap-south-1',aws_access_key_id="AKIAWK7KZTD6JH7WIOF6",
    aws_secret_access_key="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")

s3_client = boto3.client(
    "s3",
    aws_access_key_id="AKIAWK7KZTD6JH7WIOF6",
    aws_secret_access_key="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",
    region_name='ap-south-1'
)

s3_bucket_name = "vanathifinal"
for extracted_file in extracted_files:
    s3_key = os.path.basename(extracted_file)
    with open(extracted_file, "rb") as f:
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=f,
            ContentType="application/json"
        )
    print(f"Uploaded {s3_key} to Amazon S3.")

print("Data successfully uploaded to Amazon S3.")

import boto3
import json

s3_client = boto3.client('s3',region_name='ap-south-1',aws_access_key_id="AKIAWK7KZTD6JH7WIOF6",
    aws_secret_access_key="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",
    )
dynamodb_client = boto3.client('dynamodb',region_name='ap-south-1',aws_access_key_id="AKIAWK7KZTD6JH7WIOF6",
    aws_secret_access_key="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",
    )

bucket_name = 'vanathifinal'
table_name = 'sub_final'

response = s3_client.list_objects(Bucket=bucket_name)


for obj in response.get('Contents', []):
    object_key = obj['Key']

    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    json_content = response['Body'].read().decode('utf-8')


    data = json.loads(json_content)


    attribute_map = {}
    for key, value in data.items():
        attribute_map[key] = {
            'S': str(value) if isinstance(value, (str, int, float)) else json.dumps(value)
        }


    dynamodb_client.put_item(
        TableName=table_name,
        Item=attribute_map
    )

    print(f"Uploaded data from {object_key} to DynamoDB")

print("Success")
