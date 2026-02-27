import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.client('sqs')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/867267088795/Queue1'

def validate_slots(slots):
    location_slot = slots.get('Location')
    if location_slot and location_slot.get('value'):
        location = location_slot['value']['interpretedValue']
        if location.lower() != 'new york':
            return {
                'isValid': False,
                'violatedSlot': 'Location',
                'message': f"I'm sorry, I can only fulfill requests for New York. {location} is not supported."
            }
   
    date_slot = slots.get('DiningDate')
    if date_slot and date_slot.get('value'):
        res_date = date_slot['value']['interpretedValue']
        if datetime.strptime(res_date, '%Y-%m-%d').date() < datetime.today().date():
            return {
                'isValid': False,
                'violatedSlot': 'DiningDate',
                'message': "I'm sorry, but I can't book for a past date. Please enter a valid date."
            }

    return {'isValid': True}

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    invocation_source = event['invocationSource']
    slots = event['sessionState']['intent']['slots']
    
    if intent_name == "GreetingIntent":
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Fulfilled"}
            },
            "messages": [{"contentType": "PlainText", "content": "Hello! 😄 How can I help you today?"}]
        }
    
    if intent_name == "DiningSuggestionsIntent":
        if invocation_source == 'DialogCodeHook':
            validation_result = validate_slots(slots)
            
            if not validation_result['isValid']:
                return {
                    "sessionState": {
                        "dialogAction": {
                            "slotToElicit": validation_result['violatedSlot'],
                            "type": "ElicitSlot"
                        },
                        "intent": {"name": intent_name, "slots": slots}
                    },
                    "messages": [{"contentType": "PlainText", "content": validation_result['message']}]
                }
          
            return {
                "sessionState": {
                    "dialogAction": {"type": "Delegate"},
                    "intent": {"name": intent_name, "slots": slots}
                }
            }

      
        try:
            data = {
                "location": slots["Location"]["value"]["interpretedValue"],
                "cuisine": slots["Cuisine"]["value"]["interpretedValue"],
                "email": slots["Email"]["value"]["interpretedValue"],
                "date": slots["DiningDate"]["value"]["interpretedValue"],
                "time": slots["DiningTime"]["value"]["interpretedValue"],
                "count": slots["PartyCount"]["value"]["interpretedValue"]
            }
            
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(data))
            
            return {
                "sessionState": {
                    "dialogAction": {"type": "Close"},
                    "intent": {"name": intent_name, "state": "Fulfilled"}
                },
                "messages": [{"contentType": "PlainText", "content": "I have received your request and will email you shortly."}]
            }
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return {
                "sessionState": {
                    "dialogAction": {"type": "Close"},
                    "intent": {"name": intent_name, "state": "Failed"}
                },
                "messages": [{"contentType": "PlainText", "content": "Error processing request."}]
            }

    return {"sessionState": {"dialogAction": {"type": "Close"}, "intent": {"name": intent_name, "state": "Fulfilled"}}}