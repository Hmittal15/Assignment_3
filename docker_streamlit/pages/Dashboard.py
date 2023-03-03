import io
import streamlit as st
import pandas as pd
import sqlite3
import requests
import json
import datetime
# import Login

st.write(st.session_state.access_token)
if 'access_token' not in st.session_state:
    st.session_state.access_token = ''

if 'username' not in st.session_state:
    st.session_state.username = ''

username = st.session_state.username
# print(username)


# # Convert the timestamp column to a datetime format and set it as the index
# df['first_call'] = pd.to_datetime(df['first_call'])
# df.set_index('first_call', inplace=True)

# # Resample the data by user and day to get the count of requests by each user on each day
# df_count = df.groupby([pd.Grouper(freq='D'), 'username']).size().unstack(fill_value=0)

# # Create a line chart using plotly
# fig = px.line(df_count, x=df_count.index, y=df_count.columns)
# st.plotly_chart(fig)

# # Close the database connection
# conn.close()



response = requests.post('http://localhost:8090/app-api-record')
csv_string = response.content.decode('utf-8')
app_api_record_df = pd.read_csv(io.StringIO(csv_string))

response = requests.post('http://localhost:8090/user-api-record')
csv_string = response.content.decode('utf-8')
user_api_df = pd.read_csv(io.StringIO(csv_string))
# st.write(df)
# response_text = response.json
# json_str=''
# for line in response_json.iter_lines():
#     if line:
#         json_data = json.loads(line)
#         json_str = json.dumps(json_data)
#         print(json_str)
# data = json.loads(json_str)
# json = response.json()
# print(json)
app_api_df = pd.DataFrame(columns=['username', 'first_call', 'plan', 'max_count', 'total_count', 'nex_filter', 'nex_name', 'goes_filter',
                           'goes_name', 'nex_map', 'nex_cli', 'goes_cli', 'download_cli', 'success', 'failure'])
# for line in response.iter_lines():
#     if line:
#         # data = eval(line.decode('utf-8'))
#         data = pd.read_json(line.decode('utf-8'), typ='series')
#         app_api_df = app_api_df.append(data, ignore_index=True)
# st.write(app_api_df)
# csv_string = response.content.decode('utf-8')
# app_api_df = pd.read_csv(io.StringIO(csv_string))
# print(app_api_df.head())


# response = requests.get('http://localhost:8090/user-api-record')
# csv_string = response.content.decode('utf-8')
# user_api_df = pd.read_csv(io.StringIO(csv_string))

api_df = pd.concat([app_api_df, user_api_df])
st.dataframe(api_df)



st.title('Dashboard')



st.header('API METRICS')


# col1, col2, col3 = st.columns(3)

# # filter out rows for username 'john'
# filtered_df = api_df.loc[api_df['username'] == username]
# print(filtered_df)


# # assume your data is stored in a DataFrame called 'df'
# # set the username and timestamp columns as the index
# # df = df.set_index(['username', 'timestamp'])

# Convert the timestamp column to datetime
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

# Display the data using the metrics widget
st.write(user_calls)


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

# Display the data using the metrics widget
st.write(user_calls_today)


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

# Display the data using the metrics widget
st.write(user_calls_last_week)







# end_time = datetime.now()
# start_time = end_time - timedelta(hours=24)

# # filter the DataFrame to show only the selected user and timestamp range
# user_calls = df.loc[user, start_time:end_time]

# # group the filtered data by the user and sum the number of API calls
# total_calls = user_calls.groupby('username')['api_calls'].sum()

# # print the total calls made by the user in the last 24 hours
# print(total_calls[user])


# ------------------------

# col1.metric("API Calls Yesterday", "70 °F", "1.2 °F")

# # select count(success) from users.db where username = "" and time = 




# --------------------


# import pandas as pd
# from datetime import datetime

# # assume your data is stored in a DataFrame called 'df'
# # set the timestamp column as the index
# df = df.set_index('timestamp')

# # select the user and date you're interested in
# user = 'your_username'
# date = '2023-03-03' # yyyy-mm-dd format

# # filter the DataFrame to show only the selected user and date
# user_calls = df.loc[(df['username'] == user) & (df.index.date == datetime.strptime(date, '%Y-%m-%d').date())]

# # sum the number of API calls made by the user on the selected date
# total_calls = user_calls['api_calls'].sum()

# # print the total calls made by the user on the selected date
# print(total_calls)






# col2.metric("API Calls Today", "9 mph", "-8%")
# # select count(success) from users.db where username = "" and time = 

# col3.metric("Average API Calls This Week", "86%", "4%")
# # select avg(success) from users.db where username = "" and time = one week

# st.header(' ')





# st.header('Request Count By Each User Against Time')


# # req_count_vs_time = pd.DataFrame(
# #     np.random.randn(20, 3),
# #     columns=['a', 'b', 'c'])

# # st.line_chart(req_count_vs_time)



# # st.header(' ')





# st.header('Count of Success and Failed Request Calls')


# # api_df
# #edit this

# # suc_fail = pd.DataFrame(
# #     api_df[]
# #     columns=["Success", "Failed"])

# # st.bar_chart(suc_fail)

# # select count(success) from users.db where username = "" 
# # select count(failure) from users.db where username = "" 
# # st.header(' ')





# # st.header('Total Calls Per Endpoint')



# # endpoint_usage = pd.DataFrame(
# #     np.random.randn(20, 3),
# #     columns=["a", "b", "c"])

# # st.bar_chart(endpoint_usage)