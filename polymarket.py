# POLYMARKET API FORMAT EXAMPLE #

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


def fetch_polymarket_events():
    url = 'https://gamma-api.polymarket.com/events'
    params = {
        "closed": "false",
        "limit": 1000,
        "liquidity_num_min": 5000,
        "volume_num_min": 1000
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
        event_dict = {}  # Dictionary to consolidate outcomes under the same title

        for event in events:
            for market in event.get('markets', []):
                title = event.get('title', '')
                description = event.get('description', '')
                from datetime import datetime

                def safe_parse_date(date_str):
                    """ Convert ISO date string to datetime, return None if invalid. """
                    try:
                        return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None
                    except ValueError:
                        return None

                start_date = safe_parse_date(event.get('startDate'))
                end_date = safe_parse_date(event.get('endDate'))


                # Parse outcomes and prices
                outcomes = market.get('outcomes', [])
                outcome_prices = market.get('outcomePrices', [])
                specific_name = market.get('groupItemTitle', '')  # Specific name (e.g., "Ariana Grande")

                if isinstance(outcomes, str):
                    outcomes = json.loads(outcomes)
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)

                formatted_outcomes = []

                # If a specific name exists, prioritize it over Yes/No garbage
                if specific_name and len(outcome_prices) > 0:
                    try:
                        price = float(outcome_prices[0]) * 100  # Convert to percentage
                        formatted_outcomes.append({
                            "option": specific_name,
                            "yes_ask": price,
                            "no_ask": 100 - price
                        })

                    except (ValueError, TypeError):
                        pass  # Skip invalid prices

                # If outcomes and prices match in length, add them
                elif len(outcomes) == len(outcome_prices):
                    for outcome, price in zip(outcomes, outcome_prices):
                        try:
                            price = float(price) * 100  # Convert to percentage
                            formatted_outcomes.append({
                                "option": specific_name,
                                "yes_ask": price,
                                "no_ask": 100 - price
                            })
                        except (ValueError, TypeError):
                            pass  # Skip invalid prices

                # Filter out cases where any value is null
                formatted_outcomes = [entry for entry in formatted_outcomes if "" not in entry.values()]


                # If no valid outcomes, skip this market
                if not formatted_outcomes:
                    continue

                # Consolidate under the same title
                if title in event_dict:
                    event_dict[title]['outcomes'].extend(formatted_outcomes)
                else:
                    event_dict[title] = {
                        'title': title,
                        'description': description,
                        'startDate': start_date,
                        'endDate': end_date,
                        'outcomes': formatted_outcomes
                    }

        # Convert dictionary to list for DataFrame creation
        consolidated_events = list(event_dict.values())

        if not consolidated_events:
            print("No valid events found.")
            return pl.DataFrame()

        df = pl.DataFrame(consolidated_events, schema={
            "title": pl.Utf8,
            "description": pl.Utf8,
            "startDate": pl.Datetime("us"),
            "endDate": pl.Datetime("us"),
            "outcomes": pl.List(pl.Struct({  # Ensures outcomes match Kalshi's struct format
                "option": pl.Utf8,
                "yes_ask": pl.Float64,
                "no_ask": pl.Float64
            }))
        })

        
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

        print("POLYMARKET")
        print(selected_data)
        return selected_data

    except Exception as e:
        raise ValueError(f"Error transforming Polymarket data: {e}")


def get_polymarket_events():
    events = fetch_polymarket_events()

    if not events:
        print("No Polymarket events found.")
        return pl.DataFrame()

    try:
        transformed_events = transform_data(events)
        return transformed_events
    except Exception as e:
        raise ValueError(f"Error in Polymarket transformation: {e}")


# Fetch and transform Polymarket events
get_polymarket_events()
