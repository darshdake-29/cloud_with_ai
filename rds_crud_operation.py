import json
import pymysql

def connect_database():
    connection = pymysql.connect(
        host='database-1.cpibhh6klzmv.ap-south-1.rds.amazonaws.com',
        user='admin',
        password='*********',
        db='rds_database'
    )
    return connection


def insert_query(first_name, last_name, username, password):
    connection = connect_database()
    cursor1 = connection.cursor(pymysql.cursors.DictCursor)
    cursor1.execute(
        "INSERT INTO users_info(first_name, last_name, username, password) \
        VALUES ('{}', '{}', '{}', '{}')".format(first_name, last_name, \
        username, password)
        )
    connection.commit()
    connection.close()
    cursor1.close()


def view_query():
    connection = connect_database()
    cursor2 = connection.cursor(pymysql.cursors.DictCursor)
    cursor2.execute(
        "SELECT * FROM users_info"
        )
    data = cursor2.fetchall()
    connection.close()
    cursor2.close()
    return data


# def view_one_query(user_id):
#     connection = connect_database()
#     cursor3 = connection.cursor(pymysql.cursors.DictCursor)
#     cursor3.execute(
#         "SELECT * FROM users_info WHERE user_id='{}'".format(user_id)
#     )
#     data = cursor.fetchone()
#     cursor.close()
#     connection.close()
#     return data


def delete_query(user_id):
    connection = connect_database()
    cursor4 = connection.cursor(pymysql.cursors.DictCursor)
    cursor4.execute(
        "DELETE FROM users_info WHERE user_id='{}'".format(user_id)
        )
    connection.commit()
    connection.close()
    cursor4.close()
    

def lambda_handler(event, context):
    response = {}
    
    try:
        if "http_method" in event and event.get("http_method") == 'POST':
            event = event.get('body')
            
            first_name = event.get('first_name')
            last_name = event.get('last_name')
            username = event.get('username')
            password = event.get('password')
            
            if first_name and last_name and username and password:
                insert_query(first_name, last_name, username, password)
                
                response.update({
                    "status": True, 
                    "code": 200,
                    "message": "Data inserted sucessfully",
                    "data": {
                        "first_name": first_name,
                        "last_name": last_name,
                        "username": username,
                        "password": password
                    }
                })
            else:
                response.update({
                    "status": False, 
                    "code": 400,
                    "message": "Incorrect Data",
                    "data": None
                })
                
        elif "http_method" in event and event.get("http_method") == 'GET':
            # user_id = event.get("user_id")
            # data = view_one_query(user_id)
            data = view_query()
            
            if data:
                response.update({
                        "status": True, 
                        "code": 200,
                        "message": "Data fetched sucessfully",
                        "data": data
                    })
            else:
                response.update({
                    "status": False, 
                    "code": 400,
                    "message": "No Data Found",
                    "data": None
                })
                
        elif "http_method" in event and event.get("http_method") == 'DELETE':
            user_id = event.get('user_id')
            if user_id:
                delete_query(user_id)
                response.update({
                    "status": True,
                    "code": 200,
                    "message": "Record delete",
                    "data": None
                })
                
            else:
                response.update({
                    "status": False,
                    "status_code": 400,
                    "message": "Record not yet deleted",
                    "data": None
                })

    except Exception as e:
        response.update({
        "status": False,
        "code": 400,
        "message": f"Error Occured: {e}",
        "data": None
    })
    
    return response