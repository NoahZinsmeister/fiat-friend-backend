import os
import re
import json
import time
import venmo
import requests
import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

from dotenv import load_dotenv
load_dotenv()

cred = credentials.Certificate("./fiatfriends-firebase-adminsdk-8a7jb-d740e79b7f.json")
firebase_admin.initialize_app(cred, { 'databaseURL': 'https://fiatfriends.firebaseio.com/' })
ref = db.reference('venmo')

access_token = None
user_id = os.getenv("USER_ID")
username = os.getenv("USERNAME")
LIMIT = 100000
INTERVAL = 15

def initialize():
    global access_token
    
    venmo.auth.configure()
    access_token = venmo.auth.get_access_token()

def main():
    global INTERVAL

    initialize()
    while True:
        data = fetch_since()
        if data.shape[0] > 0:
            print('Incoming Transaction(s) detected...')
        time.sleep(INTERVAL)

def filter_transaction(transaction):
    if not transaction["message"].startswith("FiatFriends: "):
        return None
    elif transaction["type"] != 'payment':
        return None
    elif transaction["actor"]["username"] == username:
        print('Warning: Actor is self even after excluding non-payments.')
        return None
    elif len(transaction["transactions"]) != 1:
        print("Warning: >1 transaction found for payment.")
        return None
    elif transaction["actor"]["cancelled"]:
        print("Warning: Cancelled payment.")
        return None

    parsed_message = json.loads(transaction["message"][13:])

    return {
        "payment_id": transaction["payment_id"],
        "updated_time": transaction["updated_time"],
        "sender_username": transaction["actor"]["username"],
        "sender_picture": transaction["actor"]["picture"],
        "sender_name": transaction["actor"]["name"],
        "amount": transaction["transactions"][0]["amount"],
        "created_time": transaction["created_time"],
        "to": parsed_message['recipient'],
        "currency": parsed_message['recipientCurrency'] if 'recipientCurrency' in parsed_message else 'ETH'
    }

def fetch_since():
    global user_id, username, LIMIT, access_token

    max_since_fetched = ref.order_by_child('timestamp').limit_to_last(1).get()

    if len(max_since_fetched) == 0:
        max_since_fetched = 1552130679
    else:
        result = max_since_fetched.popitem(last=False)[1]
        if type(result) is not dict or 'timestamp' not in result:
            max_since_fetched = 1552130679
        else:
            max_since_fetched = result['timestamp']

    response = requests.get(
        f'https://venmo.com/api/v5/users/{user_id}/feed',
        params={
            'limit': LIMIT,
            'since': max_since_fetched,
        },
        headers={
            'Authorization': 'Bearer {}'.format(access_token)
        }
    )

    if not response.ok:
        raise ValueError(response.json())

    filtered_responses = [filter_transaction(transaction) for transaction in response.json()['data']]
    data = pd.DataFrame([filtered_response for filtered_response in filtered_responses if filtered_response is not None])

    for index, row in data.iterrows():
        new_tx_ref = ref.push()
        new_tx_ref.set({
            "timestamp": pd.to_datetime(row["created_time"]).value // 10**9,
            "amountFrom": row["amount"],
            "currencyFrom": 'USD',
            "currencyTo": row['currency'],
            "from": row['sender_username'],
            "fromPicture": row['sender_picture'],
            "liquidityProvider": username,
            "to": row['to']
        })

    return data

main()