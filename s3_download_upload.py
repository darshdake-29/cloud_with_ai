"""
Task to download an image from s3 bucket's folder and upload it to another folder of bucket,
on api call.
"""
import boto3

def lambda_handler(event, context):
    responce = {}
    s3_client = boto3.client('s3')

    try:
        if "http_method" in event and event.get("http_method") == 'GET':
            img = event.get("img")
            s3_client.download_file(
                "darshbucketforcv", "download/sample.png", f"/tmp/{img}")
            s3_client.upload_file(
                f"/tmp/{img}", "darshbucketforcv", f"upload/{img}")
            responce.update({
                'status': True,
                'code': 200,
                'message': "Image saved sucessfully",
		        'data': f"{img} uploaded"
            })

    except Exception as e:
        responce.update({
            'status': False,
            'code': 400,
            'message': 'Something went wrong',
	        'data': None
        })

    return responce