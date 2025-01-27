## POLYMARKET API OUTPUT EXAMPLE ##

# 'market': {'id': '519373', 
# 'question': 'Solana all time high by June 30?', 
# 'conditionId': '0x324283263c83e789fc092ed8d3333aa93ecc6ef0ba5479db05bbcb2471c92d01', 
# 'slug': 'solana-all-time-high-by-june-30', 
# 'resolutionSource': '', 
# 'endDate': '2025-06-30T12:00:00Z', 
# 'liquidity': '16566.4489', 
# 'startDate': '2025-01-21T18:52:09.491Z', 
# 'image': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/solana-all-time-high-by-june-30-OOZ6L_3iymmE.jpg', 
# 'icon': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/solana-all-time-high-by-june-30-OOZ6L_3iymmE.jpg', 
# 'description': 'This market will resolve to "Yes" if any Binance 1 minute candle for SOLUSDT between 21 Jan \'25 12:00 and 30 Jun \'25 23:59 in the ET timezone has a final “High” price that is higher than any previous Binance 1 minute candle\'s "High" price on any prior date. Otherwise, this market will resolve to "No".\n\nThe resolution source for this market is Binance, specifically the SOLUSDT "High" prices currently available at https://www.binance.com/en/trade/SOL_USDT with “1m” and “Candles” selected on the top bar.\n\nPlease note that this market is about the price according to Binance SOLUSDT, not according to other sources or spot markets.', 
# 'outcomes': '["Yes", "No"]', 
# 'outcomePrices': '["0.655", "0.345"]', 
# 'volume': '6104.492629', 
# 'active': True, 
# 'closed': False, 
# 'marketMakerAddress': '', 
# 'createdAt': '2025-01-21T18:48:14.091177Z', 
# 'updatedAt': '2025-01-27T20:26:40.881609Z', 
# 'new': False, 
# 'featured': False, 
# 'submitted_by': 
# '0x91430CaD2d3975766499717fA0D66A78D814E5c5', 
# 'archived': False, 
# 'resolvedBy': '0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74', 
# 'restricted': True, 
# 'groupItemTitle': '', 
# 'groupItemThreshold': '0', 
# 'questionID': '0x55d46b55aa1e7f59bbfedde6996e6f9048a032b5cb4add50661a669b68f1c273', 
# 'enableOrderBook': True, 
# 'orderPriceMinTickSize': 0.01, 
# 'orderMinSize': 5, 
# 'volumeNum': 6104.492629, 
# 'liquidityNum': 16566.4489, 
# 'endDateIso': '2025-06-30', 
# 'startDateIso': '2025-01-21', 
# 'hasReviewedDates': True, 
# 'volume24hr': 1276.70651,
#  'clobTokenIds': '["6611523844508119551956870980262427159329487430981844538371350199749910874741", "68626818347964286191549765628145751411975377807093035139197340370408433328002"]', 
# 'umaBond': '500', 
# 'umaReward': '5', 
# 'volume24hrClob': 1276.70651, 
# 'volumeClob': 6104.492629, 
# 'liquidityClob': 16566.4489,
# 'acceptingOrders': True, 
# 'negRisk': False, 
# 'ready': False, 
# 'funded': False, 
# 'acceptingOrdersTimestamp': 
# '2025-01-21T18:50:42Z', 
# 'cyom': False, 
# 'competitive': 0.9765386587241522, 
# 'pagerDutyNotificationEnabled': False, 
# 'approved': True, 
# 'clobRewards': [{'id': '13622', 'conditionId': '0x324283263c83e789fc092ed8d3333aa93ecc6ef0ba5479db05bbcb2471c92d01', 'assetAddress': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174', 'rewardsAmount': 0, 'rewardsDailyRate': 10, 
# 'startDate': '2025-01-21', 
# 'endDate': '2500-12-31'}], 
# 'rewardsMinSize': 50, 
# 'rewardsMaxSpread': 3.5, 
# 'spread': 0.05, 
# 'oneDayPriceChange': -0.18, 
# 'lastTradePrice': 0.68, 
# 'bestBid': 0.63, 
# 'bestAsk': 0.68, 
# 'automaticallyActive': True, 
# 'clearBookOnStart': True, 
# 'manualActivation': False, 
# 'negRiskOther': False}}


import polars as pl
import requests
import json


from datetime import datetime

def fetch_polymarket_events():
    url = 'https://gamma-api.polymarket.com/events'
    params = {
        "closed": "false",
        "limit": 1000,
        "liquidity_num_min": 5000,
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch Polymarket events. HTTP status: {response.status_code}")

    events = response.json()
    if not isinstance(events, list):
        raise ValueError(f"Unexpected response format: {type(events)}")
    return events

def transform_data(events):
    try:
        # Flatten events into individual rows
        flattened_events = [
            {
                'title': event.get('title', ''),
                'description': event.get('description', ''),
                'startDate': event.get('startDate', None),
                'endDate': event.get('endDate', None),
                'market': market,
                'id': event.get(''),
                'slug': event.get('slug', '')
            }
            for event in events
            for market in event.get('markets', [])
        ]
        # DEBUG PRINT # print(f"Flattened Events: {flattened_events}") # DEBUG PRINT

        # Consolidate outcomes and prices into Kalshi-style 'outcomes' column
        consolidated_events = []
        for event in flattened_events:
            outcomes = event['market'].get('outcomes', [])
            outcome_prices = event['market'].get('outcomePrices', [])

            # Making sure it counts the length of outcomes and outcome_prices PROPERLY
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)

            titles = event['market'].get('title', [])
            ids = event['market'].get('id', [])
            slugs = event['market'].get('slug', [])
            # DEBUG PRINT # print(f"Slug: {slugs},Title: {titles}, Outcomes: {outcomes}, Outcome Prices: {outcome_prices}")  # Debug print

            if len(outcomes) != len(outcome_prices):
                continue

            formatted_outcomes = []
            for outcome, price in zip(outcomes, outcome_prices):
                try:
                    # Format each outcome as Kalshi-style dict
                    price = float(price) * 100  # Convert to percentage
                    formatted_outcomes.append({
                        'subtitle': outcome,
                        'yes_ask': price,
                        'no_ask': 100 - price
                    })
               
                except (ValueError, TypeError) as e:
                    print(f"Skipping invalid price: {price}, Error: {e}")
                    continue  # Skip invalid prices
            
            if not formatted_outcomes:
                print(f"Skipping event due to no valid formatted outcomes: {event}")
                continue  # Skip events with no valid formatted outcomes

            consolidated_events.append({
                'title': event['title'],
                'description': event['description'],
                'startDate': event['startDate'],
                'endDate': event['endDate'],
                'outcomes': formatted_outcomes  # Kalshi-style outcomes
            })

        # Create a DataFrame from the reformatted events
        if not consolidated_events:
            print("No valid events found.")
            return pl.DataFrame()  # Return an empty DataFrame if no events

        df = pl.DataFrame(consolidated_events, strict=False)

        # Select relevant columns
        selected_data = (
            df.select([
                pl.col('title'),
                pl.col('description').alias('subtitle'),
                'outcomes',  # Consolidated outcomes column
                pl.col('startDate').cast(pl.Datetime, strict=False),
                pl.col('endDate').cast(pl.Datetime, strict=False)
            ])
        )

        # DEBUG PRINT
        # print("POLYMARKET")
        # print(selected_data)
        return selected_data

    except Exception as e:
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
