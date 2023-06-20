"""
This program will detect total amount from a bill and simply show output
if bill contains total amount then print it
else print no total amount
"""
import json
import boto3

def lambda_handler(event, context):
    response = {}
    try: 
        textract = boto3.client('textract')
        s3 = boto3.client('s3')
        
        bucket = 'darshbucketforcv'
        file_name = 'bill.png'
        document_name = '/tmp/bill.png'
        
        s3.download_file(bucket, file_name, document_name)
        
        with open(document_name, 'rb') as document:
            imageBytes = bytearray(document.read())
        
        output = textract.detect_document_text(Document={'Bytes': imageBytes})

        now = 0
        for item in output['Blocks']:
            if item['BlockType'] == 'LINE':
                # if line contains word TOTAL then set now to 1
                if item['Text'] == 'TOTAL':
                    now = 1

                # if other Rs in bill then too it won't set amount due to now
                # set amount when TOTAL word occurs
                elif now == 1 and 'Rs' in item['Text']:
                    amount = item['Text']
                    break # we got the amount why run program further?
        
                else:
                    amount = 'No amount found'
                    
        message = f"Total amount: {amount}"
        
        response.update({
            'code': 200,
            'status': True,
            'message': f'{str(message)}',
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