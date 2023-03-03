
import streamlit as st
import requests



user_bucket_name = 'damg-test'
subscription_tiers = ['free', 'gold', 'platinum']
BASE_URL = 'http://localhost:8090'


# Streamlit app
st.title('Sign-up Page')

# Collect user details

username = st.text_input('Username')
fullname = st.text_input('Full Name')
password = st.text_input('Password', type='password')
plan = st.selectbox('Plan Type', ['Free', 'Gold', 'Platinum'])


# Handle form submission
if st.button('Sign up'):
    # Make a request to the endpoint to check if the username already exists
    username_response = (requests.post(BASE_URL + f'/check-user-exists?username={username}')).json()
    status = username_response["user"]

    if status:
        st.write("Username already exists.")
        # raise typer.Abort()


    else:
        # Code to create user goes here
        requests.post(BASE_URL + f'/add-user?username={username}&password={password}&full_name={fullname}&plan={plan}')

        st.success(f"User : {username} created successfully with Name : {fullname} and Subscription Plan : {plan}.")
  
