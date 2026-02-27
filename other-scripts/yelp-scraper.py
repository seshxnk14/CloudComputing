import requests
import boto3
import time
from datetime import datetime
from decimal import Decimal

YELP_API_KEY = "j16LfViRhMUlpHpj06rKn-t4rYpKXxRIeW095QaR4Y1PAEmFBUVQGbZxgy8QjFaj_X5Vjpcsc1htXEVz5VB59DgCfGX71I0yiOAN3ae034xemEkyqa7ITtxbiIegaXYx"
TABLE_NAME = "yelp-restaurants"
REGION_NAME = "us-east-1"
LOCATION = "Manhattan"
LIMIT = 50
CUISINES = ["Mexican","Chinese","Thai","Korean","Japanese","Indian","Italian"]

dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

def fetch_restaurants(cuisine, offset):
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {"term": cuisine, "location": LOCATION, "limit": LIMIT, "offset": offset}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("businesses", [])

def insert_into_dynamo(business, cuisine):
    item = {
        "business_id": business["id"],
        "name": business.get("name", ""),
        "address": " ".join(business.get("location", {}).get("display_address", [])),
        "coordinates": {k: Decimal(str(v)) for k, v in business.get("coordinates", {}).items()},
        "review_count": business.get("review_count", 0),
        "rating": Decimal(str(business.get("rating", 0.0))),
        "zip_code": business.get("location", {}).get("zip_code", ""),
        "cuisine": cuisine,
        "insertedAtTimestamp": datetime.utcnow().isoformat()
    }
    table.put_item(Item=item)

def lambda_handler(event, context):
    total_inserted = 0
    for cuisine in CUISINES:
        offset = 0
        unique_businesses = set()
        while len(unique_businesses) < 200:
            businesses = fetch_restaurants(cuisine, offset)
            if not businesses:
                break
            for business in businesses:
                if business["id"] not in unique_businesses:
                    insert_into_dynamo(business, cuisine)
                    unique_businesses.add(business["id"])
                    total_inserted += 1
                    if len(unique_businesses) >= 200:
                        break
            offset += LIMIT
            time.sleep(0.5)
    return {"status": "completed", "total_inserted": total_inserted}