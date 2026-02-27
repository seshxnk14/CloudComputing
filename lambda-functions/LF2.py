import json
import boto3
import random
import requests
from requests_aws4auth import AWS4Auth

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key, 
    credentials.secret_key, 
    region, 
    service, 
    session_token=credentials.token
)

host = 'https://search-restaurants-7zatxjxrhjqvt3nbln5qlmfnpe.aos.us-east-1.on.aws'
index = 'restaurants'
url = f"{host}/{index}/_search"
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/867267088795/Queue1'
TABLE_NAME = "yelp-restaurants"
VERIFIED_EMAIL = "sm13107@nyu.edu"

dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses', region_name=region)
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    print("Polling SQS for messages...")
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5 
    )
    
    messages = response.get('Messages', [])
    if not messages:
        print("No messages found in queue.")
        return {"statusCode": 200, "body": "No messages to process"}

    for msg in messages:
        try:
            receipt_handle = msg['ReceiptHandle']
            body = json.loads(msg['Body'])
            
            cuisine_val = body.get('cuisine')
            email_addr = body.get('email', VERIFIED_EMAIL)
            
            print(f"Processing request for {cuisine_val}")
            query = {"size": 5, "query": {"match": {"Cuisine": cuisine_val}}}
            es_res = requests.post(url, auth=awsauth, json=query, headers={"Content-Type": "application/json"}).json()
            hits = es_res.get('hits', {}).get('hits', [])

            if not hits:
                email_body = f"No {cuisine_val} restaurants found."
            else:
                table = dynamodb.Table(TABLE_NAME)
                selected = random.sample(hits, min(3, len(hits)))
                recommendations = []
                
                for hit in selected:
                   
                    res_id = hit['_source']['RestaurantID']
                    db_res = table.get_item(Key={'business_id': res_id})
                    if 'Item' in db_res:
                        recommendations.append(db_res['Item'])

                email_body = f"Hello! Here are my {cuisine_val} suggestions:\n\n"
                for i, r in enumerate(recommendations, 1):
                    email_body += f"{i}. {r.get('name')}, at {r.get('address')}\n"

            ses.send_email(
                Source=VERIFIED_EMAIL,
                Destination={"ToAddresses": [email_addr]},
                Message={
                    "Subject": {"Data": "Restaurant Suggestions"},
                    "Body": {"Text": {"Data": email_body}}
                }
            )

           
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            print(f"Message deleted for {cuisine_val}")

        except Exception as e:
            print(f"Error processing message: {str(e)}")
            
    return {"statusCode": 200}