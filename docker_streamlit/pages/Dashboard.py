import io
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import requests

# # Connect to the SQLite database
# conn = sqlite3.connect('app_api_record.db')

# # Retrieve the data from the database
# df = pd.read_sql_query("SELECT username, first_call FROM app_api_record", conn)





# Convert the timestamp column to a datetime format and set it as the index
df['first_call'] = pd.to_datetime(df['first_call'])
df.set_index('first_call', inplace=True)

# Resample the data by user and day to get the count of requests by each user on each day
df_count = df.groupby([pd.Grouper(freq='D'), 'username']).size().unstack(fill_value=0)

# Create a line chart using plotly
fig = px.line(df_count, x=df_count.index, y=df_count.columns)
st.plotly_chart(fig)

# Close the database connection
conn.close()



response = requests.get('http://localhost:8090/app-api-record')
csv_string = response.content.decode('utf-8')
app_api_df = pd.read_csv(io.StringIO(csv_string))


response = requests.get('http://localhost:8090/user-api-record')
csv_string = response.content.decode('utf-8')
user_api_df = pd.read_csv(io.StringIO(csv_string))

api_df = pd.concat([app_api_df, user_api_df])



import streamlit as st
import pandas as pd
import numpy as np



st.title('Dashboard')



st.header('API METRICS')


col1, col2, col3 = st.columns(3)
col1.metric("API Calls Yesterday", "70 °F", "1.2 °F")
# select count(success) from users.db where username = "" and time = 

col2.metric("API Calls Today", "9 mph", "-8%")
# select count(success) from users.db where username = "" and time = 

col3.metric("Average API Calls This Week", "86%", "4%")
# select avg(success) from users.db where username = "" and time = one week

st.header(' ')





st.header('Request Count By Each User Against Time')


req_count_vs_time = pd.DataFrame(
    np.random.randn(20, 3),
    columns=['a', 'b', 'c'])

st.line_chart(req_count_vs_time)



st.header(' ')





st.header('Count of Success and Failed Request Calls')


# api_df
#edit this

suc_fail = pd.DataFrame(
    api_df[]
    columns=["Success", "Failed"])

st.bar_chart(suc_fail)

# select count(success) from users.db where username = "" 
# select count(failure) from users.db where username = "" 
st.header(' ')





st.header('Total Calls Per Endpoint')



endpoint_usage = pd.DataFrame(
    np.random.randn(20, 3),
    columns=["a", "b", "c"])

st.bar_chart(endpoint_usage)