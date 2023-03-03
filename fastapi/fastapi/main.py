import os
import sqlite3
from fastapi import FastAPI, Response
from dotenv import load_dotenv
import boto3
import base_model
import basic_func
import pandas as pd
import streamlit as st
from passlib.context import CryptContext
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
import json
from datetime import datetime, timedelta

from basic_func import get_current_user

app =FastAPI()

load_dotenv()

s3client = boto3.client('s3', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

goes18_bucket = 'noaa-goes18'
user_bucket_name = os.environ.get('USER_BUCKET_NAME')
nexrad_bucket = 'noaa-nexrad-level2'

@app.post("/token", status_code=200, tags=["Authenticate"])
async def login_for_access_token(request: OAuth2PasswordRequestForm = Depends()):
    all_data=basic_func.get_users_data()
    for i in range(len(all_data)):
        if (all_data[i]['username']==request.username):
            my_dict=all_data[i]
    user=my_dict
    # user = basic_func.authenticate_user(data, input.username, input.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not basic_func.verify_password(user['password'], request.password):
        raise HTTPException(status_code=400, detail="Invalid Password")
    
    access_token_expires = timedelta(minutes=basic_func.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = basic_func.create_access_token(
        data={"sub": user['username']}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me/", response_model=base_model.User, tags=["Authenticate"])
async def read_users_me(current_user: base_model.User = Depends(basic_func.get_current_active_user)):
    return current_user

@app.get("/list-years-nexrad", tags=["Nexrad"])
async def list_years_nexrad(get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the nexrad database
    c = basic_func.conn_filenames_nexrad()

    # Lists the years present in nexrad database
    year_list = basic_func.list_years_nexrad(c)

    # Clean up
    basic_func.conn_close(c)

    return {"year_list":year_list}


@app.post("/list-months-nexrad", tags=["Nexrad"])
async def list_months_nexrad(year: str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the nexrad database
    c = basic_func.conn_filenames_nexrad()

    # Lists the months for the selected year from nexrad database
    month_list = basic_func.list_months_nexrad(c, year)

    # Clean up
    basic_func.conn_close(c)

    return {"month_list":month_list}


@app.post("/list-days-nexrad", tags=["Nexrad"])
async def list_days_nexrad(year: str, month: str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the nexrad database
    c = basic_func.conn_filenames_nexrad()

    # Lists the days for the selected year and month from nexrad database
    days_list = basic_func.list_days_nexrad(c, year, month)

    # Clean up
    basic_func.conn_close(c)

    return {"days_list":days_list}


@app.post("/list-stations-nexrad", tags=["Nexrad"])
async def list_stations_nexrad(year: str, month: str, day:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the nexrad database
    c = basic_func.conn_filenames_nexrad()

    # Lists the stations for the selected year, month and day from nexrad database
    stations_list = basic_func.list_stations_nexrad(c, year, month, day)

    # Clean up
    basic_func.conn_close(c)

    return {"stations_list":stations_list}


@app.post("/list-files-nexrad", tags=["Nexrad"])
async def list_files_nexrad(year: str, month: str, day:str, station:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Lists the files present in the nexrad bucket for the selected year, month, day and station
    file_list = basic_func.list_filenames_nexrad(year, month, day, station)

    return {"file_list":file_list}


@app.post("/fetch-url-nexrad", tags=["Nexrad"])
async def fetch_url_nexrad(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    # Generates file path in nexrad bucket from file name
    file_path = basic_func.path_from_filename_nexrad(name)

    # Define path where the file has to be written
    user_object_key = f'logs/nexrad/{name}'

    # Copies the specified file from source bucket to destination bucket 
    basic_func.copy_to_public_bucket(nexrad_bucket, file_path, user_bucket_name, user_object_key)

    # Generates the download URL of the specified file present in the given bucket and write logs in S3
    aws_url = basic_func.generate_download_link_nexrad(user_bucket_name, user_object_key) 

    return {'url': aws_url.split("?")[0] }


@app.post("/validate-url-nexrad", tags=["Nexrad"])
async def validate_url_nexrad(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Generates file path in goes18 bucket from file name
    file_path = basic_func.path_from_filename_nexrad(name)

    # Generates the download URL of the specified file present in the given bucket
    aws_url = basic_func.generate_download_link_nexrad(nexrad_bucket, file_path) 

    return {'url': aws_url.split("?")[0] }


@app.get("/list-years-goes", tags=["GOES18"])
async def list_years_goes(get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the goes database
    c = basic_func.conn_filenames_goes()

    # Lists the years present in goes database
    year_list = basic_func.list_years_goes(c)

    # Clean up
    basic_func.conn_close(c)

    return {"year_list":year_list}


@app.post("/list-days-goes", tags=["GOES18"])
async def list_days_goes(year:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the goes database
    c = basic_func.conn_filenames_goes()

    # Lists the days for the selected year from goes database
    days_list = basic_func.list_days_goes(c, year)

    # Clean up
    basic_func.conn_close(c)

    return {"days_list":days_list}


@app.post("/list-hours-goes", tags=["GOES18"])
async def list_hours_goes(year:str, day:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Establishes a connection to the goes database
    c = basic_func.conn_filenames_goes()

    # Lists the hours for the selected year and day goes database
    hours_list = basic_func.list_hours_goes(c, year, day)

    # Clean up
    basic_func.conn_close(c)

    return {"hours_list":hours_list}


@app.post("/list-files-goes", tags=["GOES18"])
async def list_files_goes(year:str, day:str, hour:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Lists the files present in the goes18 bucket for the selected year, day and hour
    file_list = basic_func.list_filenames_goes(year, day, hour)

    return {"file_list":file_list}


@app.post("/fetch-url-goes", tags=["GOES18"])
async def fetch_url_goes(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    # Generates file path in goes18 bucket from file name
    file_path = basic_func.path_from_filename_goes(name)

    # Define path where the file has to be written
    user_object_key = f'logs/goes18/{name}'

    # Copies the specified file from source bucket to destination bucket 
    basic_func.copy_to_public_bucket(goes18_bucket, file_path, user_bucket_name, user_object_key)

    # Generates the download URL of the specified file present in the given bucket and write logs in S3
    aws_url = basic_func.generate_download_link_goes(user_bucket_name, user_object_key) 

    return {'url': aws_url.split("?")[0] }


@app.post("/validate-url-goes", tags=["GOES18"])
async def validate_url_goes(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Generates file path in goes18 bucket from file name
    file_path = basic_func.path_from_filename_goes(name)

    # Generates the download URL of the specified file present in the given bucket
    aws_url = basic_func.generate_download_link_goes(goes18_bucket, file_path) 

    return {'url': aws_url.split("?")[0] }


@app.post("/fetch-url-goes-from-name", tags=["GOES18"])
async def fetch_url_goes_from_name(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    if (name == ""): 
        return{"url":"Please enter file name"}
        

    else:

        file_integrity = basic_func.validate_file_goes(name)

        if (file_integrity == 'Valid file') :

            # Generate file path from filename
            src_object_key = basic_func.path_from_filename_goes(name)

            # Checks if the provided file exists in goes bucket
            if basic_func.check_if_file_exists_in_s3_bucket(goes18_bucket, src_object_key):

                # Define path where the file has to be written
                user_object_key = f'logs/goes18/{name}'

                # Copy file from GOES18 bucket to user bucket
                basic_func.copy_to_public_bucket(goes18_bucket, src_object_key, user_bucket_name, user_object_key)

                # Generate link from user bucket
                aws_url = basic_func.generate_download_link_goes(user_bucket_name, user_object_key)

                # Returns the generated URL
                return {'url': aws_url.split("?")[0] }
            
            else:
                # Returns a message saying file does not exist in the bucket
                return {"url":"404: File not found"}

        else:
            return {"url":file_integrity}


@app.post("/fetch-url-nexrad-from-name", tags=["Nexrad"])
async def fetch_url_nexrad_from_name(name:str,
    get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    if (name == ""): 
        return{"url":"Please enter file name"}
        

    else:
        
        file_integrity = basic_func.validate_file_nexrad(name)

        if (file_integrity == 'Valid file') :

            # Generate file path from filename
            src_object_key = basic_func.path_from_filename_nexrad(name)

            # Checks if the provided file exists in nexrad bucket
            if basic_func.check_if_file_exists_in_s3_bucket(nexrad_bucket, src_object_key):

                # Define path where the file has to be written
                user_object_key = f'logs/nexrad/{name}'

                # Copy file from nexrad bucket to user bucket
                basic_func.copy_to_public_bucket(nexrad_bucket, src_object_key, user_bucket_name, user_object_key)

                # Generate link from user bucket
                aws_url = basic_func.generate_download_link_nexrad(user_bucket_name, user_object_key)

                # Returns the generated URL
                return {'url': aws_url.split("?")[0] }
            
            else:
                # Returns a message saying file does not exist in the bucket
                return {"url":"404: File not found"}

        else:
            return {"url":file_integrity}


@app.get("/mapping-stations", tags=["Nexrad"], response_class=Response)
async def mapping_stations(response: Response,
    get_current_user: base_model.User = Depends(get_current_user)) -> str:

    # Retrieve data from database
    db = sqlite3.connect('location.db')
    cursor = db.cursor()
    cursor.execute('''SELECT lat, long, City FROM loaction_radar''')
    data = cursor.fetchall()
    
    # Create DataFrame and convert to CSV string
    df = pd.DataFrame(data, columns=["column1", "column2", "column3"])
    csv_string = df.to_csv(index=False)

    # # Set response headers and return CSV string
    response.headers["Content-Disposition"] = "attachment; filename=my_data.csv"
    response.headers["Content-Type"] = "text/csv"
    return csv_string


@app.post("/download", tags=["CLI"])
async def download(filename: str,
                   get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    """
    Downloads a file with the specified filename and returns the URL of the file moved to your S3 location.
    """

    # Checks if it is a file from GOES18 bucket
    if (filename[:16] == 'OR_ABI-L1b-RadC-'):
            
        # Generate file path from filename
        src_object_key = basic_func.path_from_filename_goes(filename)

        # Checks if the provided file exists in goes bucket
        if basic_func.check_if_file_exists_in_s3_bucket('noaa-goes18', src_object_key):

            # Define path where the file has to be written
            user_object_key = f'logs/goes18/{filename}'

            # Copy file from GOES18 bucket to user bucket
            basic_func.copy_to_public_bucket('noaa-goes18', src_object_key, user_bucket_name, user_object_key)

            # Generate link from user bucket
            aws_url = basic_func.generate_download_link_goes(user_bucket_name, user_object_key)

            # Returns the generated URL
            return {"url" : aws_url.split("?")[0]}
        
        else:
            # Returns a message saying file does not exist in the bucket
            return {"url" : "404: File not found"}

    else:

        # Generate file path from filename
        src_object_key = basic_func.path_from_filename_nexrad(filename) 

        # Checks if the provided file exists in nexrad bucket
        if basic_func.check_if_file_exists_in_s3_bucket('noaa-nexrad-level2', src_object_key):

            # Define path where the file has to be written
            user_object_key = f'logs/nexrad/{filename}'

            # Copy file from nexrad bucket to user bucket
            basic_func.copy_to_public_bucket('noaa-nexrad-level2', src_object_key, user_bucket_name, user_object_key)

            # Generate link from user bucket
            aws_url = basic_func.generate_download_link_nexrad(user_bucket_name, user_object_key)

            # Returns the generated URL
            return {"url" : aws_url.split("?")[0]}
        
        else:
            # Returns a message saying file does not exist in the bucket
            return {"url" : "404: File not found"}
            

@app.post("/fetch-goes", tags=["CLI"])
async def fetch_goes(file_prefix: str, year: str, day: str, hour: str,
                     get_current_user: base_model.User = Depends(get_current_user)) -> dict:

    # Validation for user input
    if int(year) > 2023 or int(year) < 2022:
        return {"file_list" : ['Please enter a valid year']}
    
    if int(day) > 366 or int(day) < 1:
        return {"file_list" : ['Please enter a valid day']}
    
    if int(hour) < 0 or int(hour) > 23:
        return {"file_list" : ['Please enter a valid station name']}

    # Lists the files present in the goes18 bucket for the selected year, day and hour
    file_list = basic_func.list_filenames_goes(file_prefix, year, day, hour)

    return {"file_list" : file_list}


@app.post("/fetch-nexrad", tags=["CLI"])
async def fetch_nexrad(year: str, month: str, day: str, station: str,
                       get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    
    # Validation for user input
    if int(year) > 2023 or int(year) < 1970:
        return {"file_list" : ['Please enter a valid year']}
    
    if int(month) > 12 or int(month) < 1:
        return {"file_list" : ['Please enter a valid month']}
    
    if int(day) > 31 or int(day) < 1:
        return {"file_list" : ['Please enter a valid day']}
    
    if len(station) != 4:
        return {"file_list" : ['Please enter a valid station name']}

    # Lists the files present in the nexrad bucket for the selected year, month, day and station
    file_list = basic_func.list_filenames_nexrad(year, month, day, station)

    return {"file_list" : file_list}


@app.post("/add-user", tags=["CLI"])
async def add_user(username: str, password: str, email: str, full_name: str, plan: str,
                   get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    basic_func.add_user(username, password, email, full_name, plan)

    return {"user" : "User added"}


@app.post("/check-user-exists", tags=["CLI"])
async def check_user_exists(username: str,
                            get_current_user: base_model.User = Depends(get_current_user)) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    status = basic_func.check_user_exists(username)

    return {"user" : status}


@app.post("/check-users-api-record", tags=["CLI"])
async def check_users_api_record(username: str) -> dict:

    print("hola"+username)
    status = basic_func.check_users_api_record(username)

    return {"user" : status}


@app.post("/update-users-api-record", tags=["CLI"])
async def update_users_api_record(url: str, response: str, username: str) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date

    status = basic_func.update_users_api_record(url, response, username)

    return {"user" : status}