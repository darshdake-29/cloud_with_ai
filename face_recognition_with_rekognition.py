""" 
This program will compare two faces of two different 
folders (master_collection & child collection) of s3 bucket and will check
tags of images of child collection if tag is_compared is false then it 
will compare with all images of master collection and if that image of child
collection matches to any image of master collection then tag will be 
updated i.e. is_compared to true. If image already has is_compared to true 
then it will not be compared.
"""

import json
import boto3
import os

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

def check_tags(bucket_name, file_name):
    tag = s3.get_object_tagging(
        Bucket=bucket_name,
        Key=file_name
    )
    # check tags of image of child_collection.
    # only download to /tmp if tag is_compared is false
    if tag['TagSet'][0]['Key'] == 'is_compared' and tag['TagSet'][0]['Value'] == 'False':
        s3.download_file(bucket_name, file_name, '/tmp/'+str(file_name))
        
def compare_faces(master_file, child_file):
    # compare two faces one from master and one from child at a time
    imageSource = open('/tmp/master_collection/'+str(master_file), 'rb')
    imageTarget = open('/tmp/child_collection/'+str(child_file), 'rb')

    compare_result = rekognition.compare_faces(SimilarityThreshold=95,
                                    SourceImage={'Bytes': imageSource.read()},
                                    TargetImage={'Bytes': imageTarget.read()})
                                    
    if len(compare_result['FaceMatches']) == 1:
        return True
    else:
        return False
        

def lambda_handler(event, context):
    response = {}
    try: 
        bucket_name = 'darshbucketforcv'
        folders = ['master_collection/', 'child_collection/']
        
        if os.path.exists('/tmp/master_collection'):
            pass
        else:
            os.makedirs('/tmp/master_collection')
        if os.path.exists('/tmp/child_collection'):
            pass
        else:
            os.makedirs('/tmp/child_collection')
        for folder in folders:
            result = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            files = [obj['Key'] for obj in result['Contents']]
            if len(files) > 1:
                files.pop(0)
                for file in files:
                    if file.split('/')[0] == 'child_collection':
                        check_tags(bucket_name, file)
                    else:
                        s3.download_file(bucket_name, file, '/tmp/'+str(file))
                        
        # above code will download required images from the s3 bucket to 
        # respective folder in the /tmp
        
        master_files = os.listdir('/tmp/master_collection')
        child_files = os.listdir('/tmp/child_collection')
        
        message = []
        count = 0
        
        # below code will compare two faces and return list with names of images 
        # along with whether face matches matches or not 
        for master in master_files:
            for child in child_files:
                count += 1
                comparision = compare_faces(master, child)
                
                if comparision == True:
                    # if face matches then update tag
                    s3_file_url = 'child_collection/'+str(child)
                    tags = "is_compared=True"
                    s3.upload_file(
                        Filename='/tmp/child_collection/'+str(child),
                        Bucket=bucket_name,
                        Key=s3_file_url,
                        ExtraArgs={"Tagging": tags})
                    statement = f"'Match Found For {child} and {master}"
                    message.append(statement)
                
                else:
                    statement = f"'Match Not Found For {child} and {master}"
                    message.append(statement)
                
        response.update({
            'code': 200,
            'status': True,
            'message': f'{str(message)}, str{count}',
            'data': None
        })
        
    except Exception as e:
        response.update({
            'code': 500,
            'status': False,
            'message': f'"Error:", {str(e)}',
            'data': None
            })
    
    return response