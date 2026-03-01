import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SQS client
sqs = boto3.client('sqs')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/377100338169/MyQueue'
def validate_dining_suggestions(slots):
    location = slots.get('Location')
    cuisine = slots.get('Cuisine')
    
    # Example Validation: Check if location is Manhattan
    if location and location['value']['interpretedValue'].lower() != 'manhattan':
        return {
            'isValid': False,
            'violatedSlot': 'Location',
            'message': 'We currently only support Manhattan. Please try again.'
        }
    
    # Add more validations for Cuisine, Time, etc.
    return {'isValid': True}

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    invocation_source = event['invocationSource']
    slots = event['sessionState']['intent']['slots']
    
    # 1. Handle Greeting and Thank You Intents
    if intent_name == 'GreetingIntent':
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Fulfilled"}
            },
            "messages": [{"contentType": "PlainText", "content": "Hi there, how can I help?"}]
        }

    # 2. Handle Dining Suggestions Intent
    if intent_name == 'DiningSuggestionsIntent':
        if invocation_source == 'DialogCodeHook':
            # Perform Validation
            validation_result = validate_dining_suggestions(slots)
            if not validation_result['isValid']:
                return {
                    "sessionState": {
                        "dialogAction": {
                            "type": "ElicitSlot",
                            "slotToElicit": validation_result['violatedSlot']
                        },
                        "intent": {"name": intent_name, "slots": slots}
                    },
                    "messages": [{"contentType": "PlainText", "content": validation_result['message']}]
                }
            
            # If valid, let Lex decide the next step (Delegate)
            return {
                "sessionState": {
                    "dialogAction": {"type": "Delegate"},
                    "intent": {"name": intent_name, "slots": slots}
                }
            }

        if invocation_source == 'FulfillmentCodeHook':
            # Extract data and push to SQS
            reservation_data = {
                "location": slots['Location']['value']['interpretedValue'],
                "cuisine": slots['Cuisine']['value']['interpretedValue'],
                "time": slots['DiningTime']['value']['interpretedValue'],
                "num_people": slots['NumberOfPeople']['value']['interpretedValue'],
                "email": slots['email']['value']['interpretedValue']
            }
            
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(reservation_data))
            
            return {
                "sessionState": {
                    "dialogAction": {"type": "Close"},
                    "intent": {"name": intent_name, "state": "Fulfilled"}
                },
                "messages": [{"contentType": "PlainText", "content": "You’re all set. Expect my suggestions shortly!"}]
            }

    return {"sessionState": {"dialogAction": {"type": "Delegate"}}}