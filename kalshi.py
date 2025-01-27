## KALSHI API OUTPUT EXAMPLE ##

#'event_title': 'How much solar capacity will be installed in the US this year?'
#'event_subtitle': 'In 2025'
# 'ticker': 'KXSOLAR-25-50'
# 'yes_subtitle': 'At least 50 GWdc'
# 'no_subtitle': ''
# 'yes_bid': 11
# 'yes_ask': 20
# 'last_price': 11
# 'price_delta': 0
# 'close_ts':'2026-03-31T15:00:00Z'
# 'open_ts': '2024-12-20T15:00:00Z'
# 'rulebook_variables': {}
# 'result': ''}



import requests
import polars as pl
import json
import datetime

def fetch_kalshi_events():
    url = 'https://api.elections.kalshi.com/v1/users/feed'
    response = requests.get(url)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch Kalshi events. HTTP status: {response.status_code}")
    
    data = response.json()
    return data.get('feed', [])


def transform_data(events):
    try:

        flattened_events = [
            {
                'event_title': event['event_title'],
                'event_subtitle': event['event_subtitle'],
                **market
            }
            for event in events
            for market in event.get('markets', [])
        ]
        # DEBUG PRINT #  print(f"Flattened Events: {flattened_events}")

        # Consolidating duplicates by event_title and event_subtitle
        grouped_events = {}
        for event in flattened_events:
            event_key = (event['event_title'], event['event_subtitle'])
            if event_key not in grouped_events:
                grouped_events[event_key] = {
                    'event_title': event['event_title'],
                    'event_subtitle': event['event_subtitle'],
                    'close_ts': event['close_ts'],
                    'open_ts': event['open_ts'],
                    'outcomes': []
                }
            # Add current event's options (yes_subtitle and bid/ask data) to the grouped event
            grouped_events[event_key]['outcomes'].append({
                'yes_subtitle': event['yes_subtitle'],
                'yes_ask': event['yes_ask'],
                'no_ask': 100-(event['yes_ask'])
            })

        # Convert grouped events to a list of dictionaries for DataFrame
        consolidated_events = [
            {
                'event_title': event['event_title'],
                'event_subtitle': event['event_subtitle'],
                'close_ts': event['close_ts'],
                'open_ts': event['open_ts'],
                'outcomes': event['outcomes']
            }
            for event in grouped_events.values()
        ]

        # Creating a DataFrame from the consolidated events
        df = pl.DataFrame(consolidated_events, strict=False)
        
        #  Selecting relevant columns and casting datetime fields
        selected_data = (
            df.select([
                pl.col('event_title').alias('title'),
                pl.col('event_subtitle').alias('subtitle'),
                'outcomes', # Consolidated outcomes column
                pl.col('open_ts').cast(pl.Datetime, strict=False).alias('startDate'),
                pl.col('close_ts').cast(pl.Datetime, strict=False).alias('endDate')
            ])
        )
        
        ## DEBUG PRINT 
        # print("KALSHI")
        # print(selected_data)
        return selected_data

    except Exception as e:
        raise ValueError(f"Error transforming Kalshi data: {e}")

def get_kalshi_events():
    events = fetch_kalshi_events()
    if not events:
        print("No Kalshi events found.")
        return pl.DataFrame()  # Return an empty DataFrame if no events
    
    transformed_events = transform_data(events)
    return transformed_events
    
get_kalshi_events()
