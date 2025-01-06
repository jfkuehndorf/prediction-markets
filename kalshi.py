import requests
import polars as pl
import json 
import datetime

def fetch_kalshi_events():
    url = 'https://api.elections.kalshi.com/v1/users/feed'
    response = requests.get(url)
    
    # ADDED: Check for HTTP response status
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch Kalshi events. HTTP status: {response.status_code}")
    
    data = response.json()
    return data.get('feed', [])

def transform_data(events):
    try:
        # MODIFIED: Handle potential missing 'markets' key more gracefully
        events = [
            {
                'event_title': event['event_title'],
                'event_subtitle': event['event_subtitle'],
                **market
            }
            for event in events
            for market in event.get('markets', [])  # Default to empty list if 'markets' is missing
        ]
        
        df = pl.DataFrame(events, strict=False)
        selected_data = (
            df.select([
                pl.col('event_title').alias('title'),
                'event_subtitle',
                'yes_subtitle',
                'no_subtitle',
                'yes_bid',
                'yes_ask',
                # MODIFIED: Added strict=False to handle datetime casting errors
                pl.col('close_ts').cast(pl.Datetime, strict=False),
                pl.col('open_ts').cast(pl.Datetime, strict=False)
            ])
        )
        
        return selected_data
    except Exception as e:
        # ADDED: Better error handling for transformation issues
        raise ValueError(f"Error transforming Kalshi data: {e}")

def get_kalshi_events():
    events = fetch_kalshi_events()
    # ADDED: Handle case where no events are found
    if not events:
        print("No Kalshi events found.")
        return pl.DataFrame()  # Return an empty DataFrame if no events
    
    transformed_events = transform_data(events)
    return transformed_events
    
get_kalshi_events()