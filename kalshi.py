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

        flattened_events = [
            {
                'event_title': event['event_title'],
                'event_subtitle': event['event_subtitle'],
                **market
            }
            for event in events
            for market in event.get('markets', [])
        ]
        
        # Step 2: Consolidate duplicates by event_title and event_subtitle
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

        # Step 3: Convert grouped events to a list of dictionaries for DataFrame
        consolidated_events = [
            {
                'event_title': event['event_title'],
                'event_subtitle': event['event_subtitle'],
                'close_ts': event['close_ts'],
                'open_ts': event['open_ts'],
                'outcomes': event['outcomes']  # List of options for the event
            }
            for event in grouped_events.values()
        ]

        # Step 4: Create a DataFrame from the consolidated events
        df = pl.DataFrame(consolidated_events, strict=False)
        
        # Step 5: Select relevant columns and cast datetime fields
        selected_data = (
            df.select([
                pl.col('event_title').alias('title'),
                pl.col('event_subtitle').alias('subtitle'),
                'outcomes',  # Includes the consolidated options list
                pl.col('open_ts').cast(pl.Datetime, strict=False).alias('startDate'),
                pl.col('close_ts').cast(pl.Datetime, strict=False).alias('endDate')
            ])
        )
        
        # first_row = selected_data.head(1).to_dicts()  # Convert to a list of dictionaries
        # if first_row:
        #     outcomes = first_row[0].get('outcomes', [])  # Access 'outcomes' field
        #     print("Outcomes:", outcomes)
        # else:
        #     print("No data available in selected_data.")
        print("KALSHI")
        print(selected_data)
        return selected_data

    except Exception as e:
        # Error handling for transformation issues
        raise ValueError(f"Error transforming Kalshi data: {e}")

    
# def transform_data(events):
#     try:
#         # MODIFIED: Handle potential missing 'markets' key more gracefully
#         events = [
#             {
#                 'event_title': event['event_title'],
#                 'event_subtitle': event['event_subtitle'],
#                 **market
#             }
#             for event in events
#             for market in event.get('markets', [])  # Default to empty list if 'markets' is missing
#         ]
        
#         print(events[0],events[1])
#         df = pl.DataFrame(events, strict=False)
#         selected_data = (
#             df.select([
#                 pl.col('event_title').alias('title'),
#                 'event_subtitle',
#                 'yes_subtitle',
#                 'no_subtitle',
#                 'yes_bid',
#                 'yes_ask',
#                 # MODIFIED: Added strict=False to handle datetime casting errors
#                 pl.col('close_ts').cast(pl.Datetime, strict=False),
#                 pl.col('open_ts').cast(pl.Datetime, strict=False)
#             ])
#         )
        
#         return selected_data
#     except Exception as e:
#         # ADDED: Better error handling for transformation issues
#         raise ValueError(f"Error transforming Kalshi data: {e}")

def get_kalshi_events():
    events = fetch_kalshi_events()
    # ADDED: Handle case where no events are found
    if not events:
        print("No Kalshi events found.")
        return pl.DataFrame()  # Return an empty DataFrame if no events
    
    transformed_events = transform_data(events)
    return transformed_events
    
get_kalshi_events()