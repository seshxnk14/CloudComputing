import json
import boto3

client = boto3.client('lexv2-runtime')

BOT_ID = "NBCJOV42R1"
BOT_ALIAS_ID = "TSTALIASID"
LOCALE_ID = "en_US"

def lambda_handler(event, context):

    body = json.loads(event['body'])
    message = body['message']
    session_id = body['sessionId']

    response = client.recognize_text(
        botId=BOT_ID,
        botAliasId=BOT_ALIAS_ID,
        localeId=LOCALE_ID,
        sessionId=session_id,
        text=message
    )

    messages = []
    if 'messages' in response:
        for msg in response['messages']:
            messages.append({
                "type": "unstructured",
                "unstructured": {
                    "text": msg['content']
                }
            })

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({
            "messages": messages
        })
    }