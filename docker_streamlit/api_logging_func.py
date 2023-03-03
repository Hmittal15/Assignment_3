import sqlite3
import datetime


def update_users_api_record(endpoint: str, response_status: str, userid: str):
    db1 = sqlite3.connect('users_api_record.db')
    db2 = sqlite3.connect('app_api_record.db')
    db3 = sqlite3.connect('users.db')
    
    cursor1 = db1.cursor()
    cursor2 = db2.cursor()
    cursor3 = db3.cursor()
    
    now = datetime.datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    
    update_col = ""
    if (endpoint=="/fetch-url-nexrad"):
        update_col = "nex_filter"
    elif (endpoint=="/fetch-url-nexrad-from-name"):
        update_col = "nex_name"
    elif (endpoint=="/fetch-url-goes"):
        update_col = "goes_filter"
    elif (endpoint=="/fetch-url-goes-from-name"):
        update_col = "goes_name"
    elif (endpoint=="/mapping-stations"):
        update_col = "nex_map"
    elif (endpoint=="/download"):
        update_col = "download_cli"
    elif (endpoint=="/fetch-nexrad"):
        update_col = "nex_cli"
    elif (endpoint=="/fetch-goes"):
        update_col = "goes_cli"
    
    select_q_users = f'select plan from users where username="{userid}"'
    cursor3.execute(select_q_users)
    result_plan = cursor3.fetchone()

    user_plan = ""
    max_limit = 0
    if ("free" in result_plan):
        user_plan="free"
        max_limit=10
    if ("gold" in result_plan):
        user_plan="gold"
        max_limit=15
    if ("platinum" in result_plan):
        user_plan="platinum"
        max_limit=20
    
    if ("http" in response_status):
        update_q = f'UPDATE users_api_record SET {update_col} = ((select {update_col} from users_api_record where username="{userid}") + 1), success = ((select success from users_api_record where username="{userid}") + 1) WHERE username="{userid}"'
    else:
        update_q = f'UPDATE users_api_record SET {update_col} = ((select {update_col} from users_api_record where username="{userid}") + 1), failure = ((select failure from users_api_record where username="{userid}") + 1) WHERE username="{userid}"'
        
    select_q = f'select * from users_api_record where username="{userid}"'
    insert_q_user_api = f'insert into users_api_record (username, first_call, plan, max_count, total_count, nex_filter, nex_name, goes_filter, goes_name, nex_map, nex_cli, goes_cli, download_cli, success, failure) values ("{userid}", "{now_str}", "{user_plan}", {max_limit}, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)'
    update_q_user_total_count = f'UPDATE users_api_record SET total_count = ((select total_count from users_api_record where username="{userid}") + 1) WHERE username="{userid}"'
    delete_q_user_api = f'delete from users_api_record where username = "{userid}"'
    
    cursor1.execute(select_q)
    result = cursor1.fetchall()
    
    if result!=[]:
        timedelta = now - datetime.datetime.strptime(result[0][1], '%Y-%m-%d %H:%M:%S')
        insert_q_app_api = f'insert into app_api_record (username, first_call, plan, max_count, total_count, nex_filter, nex_name, goes_filter, goes_name, nex_map, nex_cli, goes_cli, download_cli, success, failure) values {result[0]}'
    
    if result==[]:
        cursor1.execute(insert_q_user_api)
        cursor1.execute(update_q)

    elif ((result!=[]) and (timedelta.total_seconds() < 60 * 60)):
        if (result[0][4] < result[0][3]):
            cursor1.execute(update_q)
            cursor1.execute(update_q_user_total_count)
        else:
            return False
    
    elif ((result!=[]) and (timedelta.total_seconds() >= 60 * 60)):
        cursor2.execute(insert_q_app_api)
        cursor1.execute(delete_q_user_api)
        cursor1.execute(insert_q_user_api)
        cursor1.execute(update_q)
        
    
    db1.commit()
    db1.close()
    db2.commit()
    db2.close()
    db3.close()
    return True

def check_users_api_record(userid: str):
    db1 = sqlite3.connect('users_api_record.db')
    db3 = sqlite3.connect('users.db')
    
    cursor1 = db1.cursor()
    cursor11 = db1.cursor()
    cursor111 = db1.cursor()
    cursor3 = db3.cursor()
    
    select_q_users = f'select plan from users where username="{userid}"'
    cursor3.execute(select_q_users)
    result_plan = cursor3.fetchone()

    user_plan = ""
    max_limit = 0
    if ("free" in result_plan):
        user_plan="free"
        max_limit=10
    if ("gold" in result_plan):
        user_plan="gold"
        max_limit=15
    if ("platinum" in result_plan):
        user_plan="platinum"
        max_limit=20
    
    select_q = f'select * from users_api_record where username="{userid}"'
    cursor1.execute(select_q)
    result = cursor1.fetchall()
        
    if (result!=[]):
        update_q_user_total_count = f'UPDATE users_api_record SET max_count = {max_limit}, plan = "{user_plan}" WHERE username="{userid}"'
        cursor11.execute(update_q_user_total_count)
        cursor111.execute(select_q)
        updated_result = cursor111.fetchall()
        if (updated_result[0][4] >= updated_result[0][3]):
            return False
    
    db1.commit()
    db1.close()
    db3.close()
    return True