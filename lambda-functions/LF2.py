import boto3
import json
import random
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# --- CONFIGURATION (UPDATE THESE) ---
REGION = 'us-east-1'
OS_HOST = 'search-restaurant-msayj6s5odaq67ufsmfdejpcuq.aos.us-east-1.on.aws'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/377100338169/MyQueue'
DYNAMO_TABLE = 'yelp-restaurants'
SENDER_EMAIL = 'bjk9802@nyu.edu' 

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE)
    ses = boto3.client('ses')

    # 1. Pull message from SQS
    print("Step 1: Checking SQS for messages...")
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2 # Short wait to avoid timeout
    )

    if 'Messages' not in response:
        print("No messages found in queue. Exiting.")
        return {'statusCode': 200, 'body': 'Queue empty'}

    msg = response['Messages'][0]
    receipt_handle = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    
    # MATCHING YOUR JSON: {"cuisine": "Indian", "email": "...", etc}
    cuisine = body.get('cuisine')
    email_address = body.get('email')
    location = body.get('location')
    
    print(f"DEBUG: Processing request for {cuisine} in {location} for {email_address}")

    # 2. Query OpenSearch for Restaurant IDs
    print(f"Step 2: Querying OpenSearch for {cuisine}")
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, REGION, 'es')
    
    os_client = OpenSearch(
        hosts=[{'host': OS_HOST, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    query = {
        "size": 10, # Get 10 potential matches
        "query": {
            "match": {
                "Cuisine": cuisine # 'Cuisine' is usually capitalized in the OpenSearch index
            }
        }
    }

    try:
        os_res = os_client.search(index="restaurants", body=query)
        hits = os_res['hits']['hits']
        
        if not hits:
            print(f"No restaurants found for cuisine: {cuisine}")
            # Delete message so it doesn't loop forever
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return {'statusCode': 200, 'body': 'No restaurants found'}

        # Pick 3 random restaurants
        selected_hits = random.sample(hits, min(len(hits), 3))
        restaurant_ids = [hit['_source']['RestaurantID'] for hit in selected_hits]
        print(f"Found IDs: {restaurant_ids}")

    except Exception as e:
        print(f"OpenSearch Error: {str(e)}")
        return {'statusCode': 500, 'body': 'OpenSearch failure'}

    # 3. Pull Full Details from DynamoDB
    print("Step 3: Fetching details from DynamoDB...")
    recommendations = []
    for i, rid in enumerate(restaurant_ids):
        # 'Business ID' must match your DynamoDB Primary Key name exactly
        res = table.get_item(Key={'Business ID': rid})
        if 'Item' in res:
            item = res['Item']
            recommendations.append(f"{i+1}. {item['Name']}, located at {item['Address']}")

    # 4. Format and Send Email via SES
    print("Step 4: Sending email via SES...")
    message_text = f"Hello! Here are my {cuisine} restaurant suggestions for {location}:\n\n"
    message_text += "\n".join(recommendations)
    message_text += "\n\nEnjoy your meal!"

    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [email_address]},
            Message={
                'Subject': {'Data': 'Your Dining Recommendations'},
                'Body': {'Text': {'Data': message_text}}
            }
        )
        print(f"Email sent successfully to {email_address}")
        
        # 5. Success! DELETE message from SQS
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        print("Message deleted from SQS.")
        
    except Exception as e:
        print(f"SES Error: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Execution complete')
    }