TITILE:
  Data Migration and Transformation Tool for Amazon RDS 
  
Technologies:
  Python,Requests,Zipfile,boto3,pandas,Amazon S3,DynamoDB,Amazon RDS
  
Result:
  The result of following these steps should be that the data from the zip file is extracted and stored in a list of dictionaries (if you are using Python) or a DataFrame (if you are using PySpark). 
  Each dictionary or DataFrame row will represent a document from one of the JSON files in the zip file. 
  The data in the list or DataFrame will then be stored in Amazon S3 as JSON files. 
  You will be able to access these JSON files using the boto3 library or the Amazon S3 web interface.
  The data from the JSON files will also be loaded into Amazon RDS (NoSQL). You will be able to access the data in RDS using SQL queries.
  The data will be stored in a table in RDS, and the schema of the table will be determined by the structure of the JSON documents. 
  
Approach:
  To extract the data from a zip file that is available at a URL and load it into Amazon S3 and Amazon RDS (NoSQL), you can follow these steps: 
    Use the requests library to download the zip file from the URL.
    Use the zipfile module to extract the data from the zip file.
    Use the boto3 library or PySpark to store the data in Amazon S3.  
    Use the pandas library and sqlalchemy or PySpark to load the data from S3 into Amazon RDS (NoSQL).



