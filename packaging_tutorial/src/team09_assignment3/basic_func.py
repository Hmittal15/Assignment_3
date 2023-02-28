import boto3
import os
import botocore
from dotenv import load_dotenv

load_dotenv()

#Establish connection to client
s3client = boto3.client('s3', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )


# Generates file path in goes18 bucket from file name
def path_from_filename_goes(filename):

    ind = filename.index('s')
    file_path = f"ABI-L1b-RadC/{filename[ind+1: ind+5]}/{filename[ind+5: ind+8]}/{filename[ind+8: ind+10]}/{filename}"
    return file_path


# Checks if the passed file exists in the specified bucket
def check_if_file_exists_in_s3_bucket(bucket_name, file_name):
    try:
        s3client.head_object(Bucket=bucket_name, Key=file_name)
        return True

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


# Copies the specified file from source bucket to destination bucket 
def copy_to_public_bucket(src_bucket_name, src_object_key, dest_bucket_name, dest_object_key):
    copy_source = {
        'Bucket': src_bucket_name,
        'Key': src_object_key
    }
    s3client.copy_object(Bucket=dest_bucket_name, CopySource=copy_source, Key=dest_object_key)


# Generates the download URL of the specified file present in the given bucket and write logs in S3
def generate_download_link_goes(bucket_name, object_key, expiration=3600):
    response = s3client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key
        },
        ExpiresIn=expiration
    )
    # write_logs_goes(f"{[object_key.rsplit('/', 1)[-1],response]}")
    return response


# Generates file path in nexrad bucket from file name
def path_from_filename_nexrad(filename):
   
    details_list =[]
    details_list.append(filename[:4])
    details_list.append(filename[4:8])
    details_list.append(filename[8:10])
    details_list.append(filename[10:12])
    details_list.append(filename)
    file_path = f"{details_list[1]}/{details_list[2]}/{details_list[3]}/{details_list[0]}/{details_list[4]}"
    return file_path


# Generates the download URL of the specified file present in the given bucket and write logs in S3
def generate_download_link_nexrad(bucket_name, object_key, expiration=3600):
    response = s3client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key
        },
        ExpiresIn=expiration
    )
    # write_logs_nexrad(f"{[object_key.rsplit('/', 1)[-1],response]}")
    return response


# Lists the files present in the goes18 bucket for the selected year, day and hour
def list_filenames_goes(file_prefix, year, day, hour):
    result = s3client.list_objects(Bucket='noaa-goes18', Prefix=f"{file_prefix}/{year}/{day}/{hour}/")
    file_list = []
    files = result.get("Contents", [])
    for file in files:
        file_list.append(file["Key"].split('/')[-1])
    return file_list


# Lists the files present in the nexrad bucket for the selected year, month, day and station
def list_filenames_nexrad(year, month, day, station):
    result = s3client.list_objects(Bucket='noaa-nexrad-level2', Prefix=f"{year}/{month}/{day}/{station}/")
    file_list = []
    files = result.get("Contents", [])
    for file in files:
        file_list.append(file["Key"].split('/')[-1])
    return file_list