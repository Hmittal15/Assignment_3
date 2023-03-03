import requests
import streamlit as st
import os
from requests.exceptions import HTTPError
import time
from api_logging_func import check_users_api_record, update_users_api_record

if 'access_token' not in st.session_state:
    st.session_state.access_token = ''

def nexrad(my_token):
    headers = {"Authorization": f"Bearer {my_token}"}
    st.markdown("<h1 style='text-align: center;'>NEXRAD</h1>", unsafe_allow_html=True)
    st.header("")
    st.header("Search by Fields")
    st.header("")


    BASE_URL = "http://localhost:8090"


    col1, col2, col3, col4 = st.columns(4, gap="large")


    # Make a request to the endpoint to retrieve the list of years
    year_response = (requests.get(BASE_URL + '/list-years-nexrad', headers=headers)).json()
    year_list = year_response["year_list"]


    with col1:
        year = st.selectbox(
            'Select the Year :',
            (year_list))
        st.write('You selected :', year)


    # Make a request to the endpoint to retrieve the list of months for the selected year
    month_response = requests.post(BASE_URL + f'/list-months-nexrad?year={year}', headers=headers).json()
    month_list = month_response["month_list"]

    
    with col2:
        month = st.selectbox(
            'Select the Month :',
            (month_list))

        st.write('You selected :', month)


    # Make a request to the endpoint to retrieve the list of days for the selected year and month
    day_response = requests.post(BASE_URL + f'/list-days-nexrad?year={year}&month={month}', headers=headers).json()
    day_list = day_response["days_list"]


    with col3:
        day = st.selectbox(
            'Select the Date :',
            (day_list))

        st.write('You selected :', day)


    # Make a request to the endpoint to retrieve the list of stations for the selected year and day
    station_response = (requests.post(BASE_URL + f'/list-stations-nexrad?year={year}&month={month}&day={day}', headers=headers)).json()
    station_list = station_response['stations_list']


    with col4:

        station = st.selectbox(
            'Select the Station :',
            (station_list))

        st.write('You selected :', station)


    st.header("")


    # Make a request to the endpoint to retrieve the list of files for the selected year, day
    file_list_response = (requests.post(BASE_URL + f'/list-files-nexrad?year={year}&month={month}&day={day}&station={station}', headers=headers)).json()
    file_list = file_list_response["file_list"]


    selected_file = st.selectbox("Select link for download", 
                (file_list),  
                key=None, help="select link for download")


    st.header("")


    if st.button('Generate using Filter'):

        # Set up the headers for authenticated requests
        headers = {"Authorization": f"Bearer {my_token}"}
        
        if (check_users_api_record(st.session_state.username)):

            # Make a request to the endpoint to fetch the url for the selected file
            file_url_response = requests.post(BASE_URL + f'/fetch-url-nexrad?name={selected_file}', headers=headers).json()
            file_url = file_url_response["url"]

            # Display the url for the selected file
            st.write('Download Link : ', file_url)

            st.write(file_url_response)

            # Make a request to the endpoint to fetch the url for the selected file from the Nexrad bucket for validation
            validation_url_response = requests.post(BASE_URL + f'/validate-url-nexrad?name={selected_file}', headers=headers).json()
            validation_url = validation_url_response["url"]
            
            st.write("NOAA bucket path for verfication : ", validation_url)

            update_users_api_record("/fetch-url-nexrad", file_url, st.session_state.username)

        else:
            st.text("User limit reached! Please try later.")

    st.header("")
    st.header("Search by Filename")
    st.header("")


    filename = st.text_input('Enter Filename')
    st.header("")



    if st.button('Generate using Name'):

        # Set up the headers for authenticated requests
        headers = {"Authorization": f"Bearer {my_token}"}

        if (check_users_api_record(st.session_state.username)):

            try:
                # Copies the file from Nexrad bucket to User bucket and generates download URL
                file_url_response = requests.post(BASE_URL + f'/fetch-url-nexrad-from-name?name={filename}', headers=headers).json()
                file_url = file_url_response["url"]    

                # Display the url for the selected file
                st.write('Download Link : ', file_url)

                update_users_api_record("/fetch-url-nexrad-from-name", file_url, st.session_state.username)

            except Exception as error:
                st.error(f"Error: {error}")

        else:
            st.text("User limit reached! Please try later.")

if "access_token" not in st.session_state or st.session_state['access_token']=='':
    st.title("Please sign-in to access this feature!")
else:
    nexrad(st.session_state["access_token"])