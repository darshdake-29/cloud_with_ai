"""
In this file, there are two tasks accomplished i.e. one to detect faces
uploaded to S3 and store the emotions data to database and other is to compare
two faces uploaded to S3 and store the similarities between faces to database.
"""

import json
import boto3
import pymysql

def connect_database():
    # It will be used to establish connection to database
    connection = pymysql.connect(
        host="darshdakedb.cpibhh6klzmv.ap-south-1.rds.amazonaws.com",
        user='admin',
        password='Darsh2972',
        db='face_rekognition',
        cursorclass=pymysql.cursors.DictCursor
        )
    return connection
        
def insert_detection_data(s3_file_path, face_accuracy, angry, calm, confused, disgusted, fear, happy, sad, surprised):
    # Store data to database when face is detected
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO detection_table(s3_file_path, face_accuracy, angry, calm, \
        confused, disgusted, fear, happy, sad, surprised) VALUES ('{}', '{}', \
        '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(s3_file_path, \
        face_accuracy, angry, calm, confused, disgusted, fear, happy, sad, \
        surprised)
        )
    connection.commit()
    connection.close()
    cursor.close()

def insert_comparision_data(s3_source_path, s3_target_path, face_similarity, left, top, faces_matched):
    # Store data to database when two faces are needed to be compared
    connection = connect_database()
    cursor3 = connection.cursor()
    cursor3.execute(
        f"INSERT INTO comparision_table(s3_source_path, s3_target_path, \
        face_similarity, left_cordinates, top_cordinates, faces_matched)\
        VALUES ('{s3_source_path}', '{s3_target_path}', \
        '{face_similarity}', '{left}', '{top}', '{faces_matched}')"
        )
    connection.commit()
    connection.close()
    cursor3.close()
    
def view_detection_data():
    # Retrieve stored emotions data of detected faces from the database
    connection = connect_database()
    cursor2 = connection.cursor()
    cursor2.execute(
        "SELECT * FROM detection_table"
        )
    data = cursor2.fetchall()
    connection.close()
    cursor2.close()
    return data
    
def view_comparision_data():
    # Retrieve comparision data of two faces
    connection = connect_database()
    cursor4 = connection.cursor()
    cursor4.execute(
        "SELECT * FROM comparision_table"
        )
    data = cursor4.fetchall()
    connection.close()
    cursor4.close()
    return data
    
def detect_face(event):
    """
    It will be executed when an image uploaded to s3 bucket in detection
    folder and will detect the face in image and will store required data to
    database
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    s3.download_file(bucket, key, '/tmp/sample.jpg')
    
    with open('/tmp/sample.jpg', 'rb') as image:
        image_bytes = image.read()
        
    detection_response = rekognition.detect_faces(
        Image={'Bytes': image_bytes},
        Attributes=['ALL'] # DEFAULT lm, bb, pose, ALL, EMOTIONS
    )
    
    type_confidence = {d['Type']: d['Confidence'] for d in detection_response['FaceDetails'][0]['Emotions']}
    angry = round(type_confidence['ANGRY'], 3)
    calm = round(type_confidence['CALM'], 3)
    confused = round(type_confidence['CONFUSED'], 3)
    disgusted = round(type_confidence['DISGUSTED'], 3)
    fear = round(type_confidence['FEAR'], 3)
    happy = round(type_confidence['HAPPY'], 3)
    sad = round(type_confidence['SAD'], 3)
    surprised = round(type_confidence['SURPRISED'], 3)
    accuracy = round(detection_response['FaceDetails'][0]['Confidence'], 3)        
        
    s3_file_path = f's3://{bucket}/{key}'
    
    insert_detection_data(s3_file_path, accuracy, angry, calm, confused, disgusted, fear, happy, sad, surprised)
    
def compare_faces(event):
    """
    It will be executed when two images are uploaded to comparision folder
    of s3 bucket and will compare those two faces and store the required
    data to the database
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    source = event['Records'][0]['s3']['object']['key']
    s3.download_file(bucket, source, f'/tmp/source.jpg')
    target = event['Records'][1]['s3']['object']['key']
    s3.download_file(bucket, target, f'/tmp/target.jpg')
    
    imageSource = open('/tmp/source.jpg', 'rb')
    imageTarget = open('/tmp/target.jpg', 'rb')
    
    response = rekognition.compare_faces(SimilarityThreshold=80,
                                    SourceImage={'Bytes': imageSource.read()},
                                    TargetImage={'Bytes': imageTarget.read()})
                                    
    for faceMatch in response['FaceMatches']:
        left = faceMatch['Face']['BoundingBox']['Left']
        top = faceMatch['Face']['BoundingBox']['Top']
        similarity = str(faceMatch['Similarity'])
    imageSource.close()
    imageTarget.close()
    faces_matched = len(response['FaceMatches'])
    s3_source_path = f's3://{bucket}/{source}'
    s3_target_path = f's3://{bucket}/{target}'
    insert_comparision_data(s3_source_path, s3_target_path, similarity, left, top, faces_matched)

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

def lambda_handler(event, context):
    response = {}
    try: 
        if 'http_method' in event and event.get('http_method') == 'GET' and event['resource'] == '/detection':
            data = view_detection_data()
            response.update({
            'code': 200,
            'status': True,
            'message': f'Data fetched successfully',
            'data': f'{data}'
            })
            return response
            
        elif 'http_method' in event and event.get('http_method') == 'GET' and event['resource'] == '/comparision':
            data = view_comparision_data()
            response.update({
            'code': 200,
            'status': True,
            'message': f'Data fetched successfully',
            'data': f'{data}'
            })
            return response
            
    except Exception as e:
        response.update({
            'code': 500,
            'status': False,
            'message': f'"Error:", {e}',
            'data': None
            })
        return response
    
    try:
        if 'Records' in event and len(event['Records']) > 0:
            key = event['Records'][0]['s3']['object']['key']
            
            if key.split('/')[0] == 'detection':
                detect_face(event)
                
            elif key.split('/')[0] == 'comparision': 
                compare_faces(event)
                
    except Exception as e:
        response.update({
            'code': 500,
            'status': False,
            'message': f'e, {e}',
            'data': None
            })

    return response