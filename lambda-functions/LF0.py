import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Incoming Event: {json.dumps(event)}")
    
    # 1. NORMALIZE THE DATA
    # Handle cases where API Gateway sends 'body' as a string (Proxy Integration)
    body = event
    if isinstance(event, dict) and 'body' in event:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    elif isinstance(event, str):
        body = json.loads(event)

    # 2. EXTRACT USER MESSAGE
    try:
        # Matches frontend format: {"messages": [{"unstructured": {"text": "..."}}]}
        messages = body.get('messages', [{}])
        user_text = messages[0].get('unstructured', {}).get('text', '').strip()
    except (AttributeError, IndexError):
        user_text = ""

    # 3. SAFETY FALLBACK (Prevents "min length: 1" error)
    if not user_text:
        user_text = "Hi" 
        logger.warning("Empty text detected. Using fallback 'Hi'")

    # 4. CONNECT TO LEX V2
    client = boto3.client('lexv2-runtime')
    
    BOT_ID = 'BT4JPDFKXU'
    ALIAS_ID = 'TSTALIASID'
    
    try:
        response = client.recognize_text(
            botId=BOT_ID,
            botAliasId=ALIAS_ID,
            localeId='en_US',
            sessionId='test_session_user',
            text=user_text
        )
        
        # 5. PARSE LEX RESPONSE
        lex_messages = response.get('messages', [])
        bot_reply = lex_messages[0].get('content', "I didn't quite get that.") if lex_messages else "Lex processed but sent no message."

        # 6. RETURN FORMATTED RESPONSE
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' # Required for Frontend CORS
            },
            'body': json.dumps({ # Wrap in json.dumps for Proxy Integration
                'messages': [
                    {
                        'type': 'unstructured',
                        'unstructured': {
                            'text': bot_reply
                        }
                    }
                ]
            })
        }

    except Exception as e:
        logger.error(f"Lex Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})

        }
