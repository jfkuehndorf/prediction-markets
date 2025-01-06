import polars as pl
import requests
import json
from datetime import datetime

def fetch_polymarket_events():
    url = 'https://gamma-api.polymarket.com/events'
    params = {
        "closed": "false",
        "limit": 1000,
        "liquidity_num_min": 5000
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch Polymarket events. HTTP status: {response.status_code}")
    
    # Parse the JSON response
    events = response.json()  # Assuming the response is a list of events
    if not isinstance(events, list):
        raise ValueError(f"Unexpected response format: {type(events)}")
    
    return events  # Return the list of events directly


def transform_data(events):
    try:
        # MODIFIED: Handle missing 'markets' key more gracefully
        events = [
            {
                'title': event['title'],
                'description': event['description'],
                'startDate': event['startDate'],
                'endDate': event.get('endDate', None),
                **market
            }
            for event in events
            for market in event.get('markets', [])  # Default to empty list if 'markets' is missing
        ]
        
        df = pl.DataFrame(events, strict=False)
        selected_data = (
            df.select([
                'title',
                'description',
                'outcomes',
                'outcomePrices',
                'bestBid',
                'bestAsk'
            ])
        )
        
        return selected_data
    except Exception as e:
        # ADDED: Better error handling for transformation issues
        raise ValueError(f"Error transforming Polymarket data: {e}")

def get_polymarket_events():
    events = fetch_polymarket_events()
    if not events:
        print("No Polymarket events found.")
        return pl.DataFrame()  # Return an empty DataFrame if no events
    
    try:
        transformed_events = transform_data(events)
        return transformed_events
    except Exception as e:
        raise ValueError(f"Error in Polymarket transformation: {e}")



get_polymarket_events()