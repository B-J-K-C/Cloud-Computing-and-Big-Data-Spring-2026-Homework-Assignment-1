import requests
import boto3
import time
from decimal import Decimal
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# --- CONFIGURATION ---
YELP_API_KEY = 'II3x41I8PA7CineIvomA5KHeQyqeJ3HEZIoREDaG-SV0oKF605Ol1EfGwhfMKs_xFGrGllx_jONBtzdY0lWElF_38LU62_I-N3WkJ0OxuQ_fNnPUkhrBrUm0bSCjaXYx'
OS_HOST = 'search-restaurant-msayj6s5odaq67ufsmfdejpcuq.aos.us-east-1.on.aws'
REGION = 'us-east-1'
CUISINES = ['Italian', 'Japanese', 'Mexican', 'Chinese', 'Indian']

# --- INITIALIZE AWS CLIENTS ---
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table('yelp-restaurants')

# OpenSearch Auth setup
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, REGION, 'es')
os_client = OpenSearch(
    hosts=[{'host': OS_HOST, 'port': 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def float_to_decimal(obj):
    """
    Recursively converts floats to Decimals. 
    DynamoDB does not support float types; it requires Decimal.
    """
    if isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        # Convert to string first to prevent floating point inaccuracies
        return Decimal(str(obj))
    return obj

def scrape_yelp():
    for cuisine in CUISINES:
        print(f"--- Starting Scraping for: {cuisine} ---")
        
        # Yelp allows 50 results per request. We loop to get 200 total per cuisine.
        for offset in range(0, 200, 50):
            url = "https://api.yelp.com/v3/businesses/search"
            headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
            params = {
                'location': 'Manhattan',
                'term': cuisine,
                'limit': 50,
                'offset': offset
            }

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"Request failed: {e}")
                break

            if 'businesses' not in data or not data['businesses']:
                print(f"No more data for {cuisine}.")
                break

            for biz in data['businesses']:
                # 1. Prepare the raw data dictionary
                raw_item = {
                    'Business ID': biz['id'],
                    'Name': biz['name'],
                    'Address': ", ".join(biz['location'].get('display_address', [])),
                    'Cuisine': cuisine,
                    'Rating': biz['rating'],  # Will be converted to Decimal
                    'Review Count': biz['review_count'],
                    'Coordinates': biz['coordinates'], # Contains floats
                    'Zip Code': biz['location'].get('zip_code', 'N/A'),
                    'insertedAtTimestamp': datetime.now().isoformat()
                }

                # 2. FIX: Convert all floats to Decimals for DynamoDB
                clean_item = float_to_decimal(raw_item)

                try:
                    # Push full record to DynamoDB
                    table.put_item(Item=clean_item)

                    # Push minimal record to OpenSearch
                    os_client.index(
                        index="restaurants", 
                        body={
                            'RestaurantID': biz['id'], 
                            'Cuisine': cuisine
                        }
                    )
                except Exception as e:
                    print(f"Error saving {biz['id']}: {e}")

            print(f"Successfully processed offset {offset} for {cuisine}")
            # Brief sleep to respect Yelp's rate limits
            time.sleep(0.5)

    print("All scraping completed successfully!")

if __name__ == "__main__":

    scrape_yelp()
