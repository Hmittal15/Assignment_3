import io
import streamlit as st
import pandas as pd
import sqlite3
import requests
import json
import numpy as np

import datetime
# import Login


if 'access_token' not in st.session_state:
    st.session_state.access_token = ''

if 'username' not in st.session_state:
    st.session_state.username = ''

username = st.session_state.username
# print(username)
if "access_token" not in st.session_state or st.session_state['access_token']=='':
    st.title("Please sign-in to access this feature!")
else:
    if(username!='admin'):
      



        response = requests.post('http://localhost:8090/app-api-record')
        csv_string = response.content.decode('utf-8')
        app_api_record_df = pd.read_csv(io.StringIO(csv_string))

        response = requests.post('http://localhost:8090/user-api-record')
        csv_string = response.content.decode('utf-8')
        user_api_df = pd.read_csv(io.StringIO(csv_string))
        
        app_api_df = pd.DataFrame(columns=['username', 'first_call', 'plan', 'max_count', 'total_count', 'nex_filter', 'nex_name', 'goes_filter',
                                'goes_name', 'nex_map', 'nex_cli', 'goes_cli', 'download_cli', 'success', 'failure'])
     
        api_df = pd.concat([app_api_df, user_api_df])
        # st.dataframe(api_df)



        st.title('Dashboard')



        st.header('API METRICS')


     
        api_df["first_call"] = pd.to_datetime(api_df["first_call"])

        # Get the date for the day before
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.date()

        # Filter the DataFrame to only include data from the day before
        yesterday_data = api_df[api_df["first_call"].dt.date == yesterday]
        yesterday_data = yesterday_data[yesterday_data['username'] == username]
        no_of_calls = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls['username'] = yesterday_data['username']
        no_of_calls['no_of_calls'] = yesterday_data['success'] + yesterday_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls = no_of_calls.groupby("username")['no_of_calls'].sum()
       

        # Convert the timestamp column to datetime
        api_df["first_call"] = pd.to_datetime(api_df["first_call"])

        # Get the date for the day before
        today = datetime.datetime.now()
        today = today.date()

        # Filter the DataFrame to only include data from the day before
        today_data = api_df[api_df["first_call"].dt.date == today]
        today_data = today_data[today_data['username'] == username]
        no_of_calls_today = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls_today['username'] = today_data['username']
        no_of_calls_today['no_of_calls'] = today_data['success'] + today_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls_today = no_of_calls_today.groupby("username")['no_of_calls'].sum()

       
        # Get the date range for last week
        today = datetime.datetime.now().date()
        last_week_start = today - datetime.timedelta(days=today.weekday() + 7)
        last_week_end = today - datetime.timedelta(days=today.weekday() + 1)

        last_week_data = api_df[api_df["first_call"].dt.date >= last_week_start]
        last_week_data = last_week_data[last_week_data["first_call"].dt.date <= last_week_end]
        last_week_data = last_week_data[last_week_data['username'] == username]

        no_of_calls_last_week = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls_last_week['username'] = last_week_data['username']
        no_of_calls_last_week['no_of_calls'] = last_week_data['success'] + last_week_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls_last_week = no_of_calls_last_week.groupby("username")['no_of_calls'].sum()

       


        # col1, col2, col3 = st.columns(3)
        # col1.metric("Total API Calls from Yesterday", user_calls, "")
        # col2.metric("Total API Calls from Today", user_calls_today,"")
        # col3.metric("Total API Calls from Last Week", user_calls_last_week, "")

        st.write("Total API Calls from Yesterday :")
        st.write(user_calls)
        st.write("")

        st.write("Total API Calls from Today :")
        st.write(user_calls_today)
        st.write("")

        st.write("Total API Calls from Last Week :")
        st.write(user_calls_last_week)
        st.header("")



        st.header("Total Number of Success and Failure API Calls")

        # Group the data by username and calculate the number of success and failure API calls
        suc_fail_data = api_df[["success", "failure"]].sum()

        st.bar_chart(data=suc_fail_data)
        st.header("")

        st.header("Total Number of API Calls based on Endpoints")


        endpoint_data = api_df[api_df['username'] == username]
        endpoint_data = endpoint_data[["nex_filter", "nex_name", "goes_filter", "goes_name", "nex_map", "nex_cli", "goes_cli", "download_cli"]]
        # st.dataframe(endpoint_data)
        st.bar_chart(data=endpoint_data)





    else:
        response = requests.post('http://localhost:8090/app-api-record')
        csv_string = response.content.decode('utf-8')
        app_api_record_df = pd.read_csv(io.StringIO(csv_string))

        response = requests.post('http://localhost:8090/user-api-record')
        csv_string = response.content.decode('utf-8')
        user_api_df = pd.read_csv(io.StringIO(csv_string))
        
        app_api_df = pd.DataFrame(columns=['username', 'first_call', 'plan', 'max_count', 'total_count', 'nex_filter', 'nex_name', 'goes_filter',
                                'goes_name', 'nex_map', 'nex_cli', 'goes_cli', 'download_cli', 'success', 'failure'])
       

      

        api_df = pd.concat([app_api_df, user_api_df])
        # st.dataframe(api_df)



        st.title('Dashboard')



        st.header('API METRICS')


        api_df["first_call"] = pd.to_datetime(api_df["first_call"])

        # Get the date for the day before
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.date()

        # Filter the DataFrame to only include data from the day before
        yesterday_data = api_df[api_df["first_call"].dt.date == yesterday]
        no_of_calls = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls['username'] = yesterday_data['username']
        no_of_calls['no_of_calls'] = yesterday_data['success'] + yesterday_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls = no_of_calls.groupby("username")['no_of_calls'].sum()


        # Convert the timestamp column to datetime
        api_df["first_call"] = pd.to_datetime(api_df["first_call"])

        # Get the date for the day before
        today = datetime.datetime.now()
        today = today.date()

        # Filter the DataFrame to only include data from the day before
        today_data = api_df[api_df["first_call"].dt.date == today]
        no_of_calls_today = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls_today['username'] = today_data['username']
        no_of_calls_today['no_of_calls'] = today_data['success'] + today_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls_today = no_of_calls_today.groupby("username")['no_of_calls'].sum()



        # Get the date range for last week
        today = datetime.datetime.now().date()
        last_week_start = today - datetime.timedelta(days=today.weekday() + 7)
        last_week_end = today - datetime.timedelta(days=today.weekday() + 1)

        last_week_data = api_df[api_df["first_call"].dt.date >= last_week_start]
        last_week_data = last_week_data[last_week_data["first_call"].dt.date <= last_week_end]

        no_of_calls_last_week = pd.DataFrame(columns= ['username', 'no_of_calls'])
        no_of_calls_last_week['username'] = last_week_data['username']
        no_of_calls_last_week['no_of_calls'] = last_week_data['success'] + last_week_data['failure']
        # Group the data by username and sum the number of API calls
        user_calls_last_week = no_of_calls_last_week.groupby("username")['no_of_calls'].sum()


        # col1, col2, col3 = st.columns(3)
        # col1.metric("Total API Calls from Yesterday", user_calls, "")
        # col2.metric("Total API Calls from Today", user_calls_today,"")
        # col3.metric("Total API Calls from Last Week", user_calls_last_week, "")

        st.write("Total API Calls from Yesterday :")
        st.write(user_calls)
        st.write("")

        st.write("Total API Calls from Today :")
        st.write(user_calls_today)
        st.write("")

        st.write("Total API Calls from Last Week :")
        st.write(user_calls_last_week)
        st.header("")




        st.header("Total Number of Success and Failure API Calls")


        # Group the data by username and calculate the number of success and failure API calls
        suc_fail_data = api_df[["success", "failure"]].sum()

        st.bar_chart(data=suc_fail_data)

        st.header("")

        st.header("Total Number of API Calls based on Endpoints")



        endpoint_data = api_df[["nex_filter", "nex_name", "goes_filter", "goes_name", "nex_map", "nex_cli", "goes_cli", "download_cli"]]
        # st.dataframe(endpoint_data)
        st.bar_chart(data=endpoint_data)
