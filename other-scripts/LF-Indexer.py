import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def lambda_handler(event, context):
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
    
 
    host = 'search-restaurants-7zatxjxrhjqvt3nbln5qlmfnpe.aos.us-east-1.on.aws' 
    index_name = 'restaurants'

    os_client = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants') 
    
  
    cuisine_counts = {}
    LIMIT = 30
    total_indexed = 0

    print("Starting smart indexing with 30-per-cuisine limit...")
    
    try:
        response = table.scan()
        items = response['Items']
        
        for item in items:
            cuisine = item.get('cuisine', 'Unknown').lower()
      
            if cuisine not in cuisine_counts:
                cuisine_counts[cuisine] = 0
    
            if cuisine_counts[cuisine] < LIMIT:
                document = {
                    "RestaurantID": item['business_id'],
                    "Cuisine": cuisine
                }
                
                os_client.index(
                    index=index_name,
                    body=document,
                    id=item['business_id'],
                    refresh=True
                )
                
                cuisine_counts[cuisine] += 1
                total_indexed += 1
            
        print(f"Indexing Summary: {cuisine_counts}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully indexed {total_indexed} total restaurants (Max 30 per cuisine).')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f"Failed: {str(e)}")}