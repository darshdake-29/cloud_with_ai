"""
Task is to download video from s3 bucket save to /tmp foldder of lambda,
using opwncv-python change codec of video and then,
upload back to s3 bucket
"""
import json
import boto3
import os
import cv2

s3 = boto3.client('s3')

def lambda_handler(event, context):
    download_bucket = 'darshbucketforcv'
    upload_bucket = 'darshmlaicv'
    key = 'sample_one.mp4'

    try:
        s3.download_file(download_bucket, key, '/tmp/sample.mp4')

        cap = cv2.VideoCapture('/tmp/sample.mp4')
        
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter('/tmp/new_video.mov', fourcc, 30.0, (int(cap.get(3)), int(cap.get(4))))
        while(cap.isOpened()):
            ret, frame = cap.read()
            if ret==True:
                out.write(frame)
            else:
                break
        cap.release()
        out.release()

        s3.upload_file('/tmp/new_video.mov', upload_bucket, 'new_video.mov')

        response = {
            'code': 200,
            'status': True,
            'message': "Video Downloaded and Uploaded successfully",
            'data': None
            }
        
    except Exception as e:
        response = {
            'code': 500,
            'status': False,
            'message': e,
            'data': None
            }

    return response