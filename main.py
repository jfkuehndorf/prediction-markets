from polymarket import get_polymarket_events
from kalshi import get_kalshi_events
from rapidfuzz import fuzz
import polars as pl
from typing import List, Dict, Optional

def compare_events(similarity_threshold: int = 75, sort_by_similarity: bool = True):
    # Fetch event data
    kalshi_events = get_kalshi_events()
    polymarket_events = get_polymarket_events()

    # Check if dataframes contain valid data
    if kalshi_events.is_empty() or polymarket_events.is_empty():
        print("One or both datasets are empty. No events to compare.")
        return []

    # Validate the presence of the 'title' column
    if 'title' not in kalshi_events.columns or 'title' not in polymarket_events.columns:
        raise ValueError("Both Kalshi and Polymarket data must contain a 'title' column.")

    # Extract and preprocess titles into lists
    kalshi_titles = kalshi_events['title'].to_list()
    poly_titles = polymarket_events['title'].to_list()

    matches = []
    for k_idx, kalshi_title in enumerate(kalshi_titles):
        for p_idx, poly_title in enumerate(poly_titles):
            similarity_score = fuzz.ratio(kalshi_title.lower(), poly_title.lower())
            if similarity_score >= similarity_threshold:
                if len(kalshi_events['outcomes']) <= 2 and len(polymarket_events['outcomes']) <= 2: # Checking if options are the same length, if not will not add to matches
                    matches.append({
                        "kalshi_title": kalshi_title,
                        "poly_title": poly_title,
                        "similarity_score": similarity_score,
                        "kalshi_index": k_idx,
                        "poly_index": p_idx,
                })

    # Debug: Print all matches before sorting
    print("\n[DEBUG] Matches Before Sorting:")
    for match in matches:
        print(match)

    if sort_by_similarity:
        matches.sort(key=lambda x: x["similarity_score"], reverse=True)

    # Debug: Print all matches after sorting
    print("\n[DEBUG] Matches After Sorting:")
    for match in matches:
        print(match)

    return matches



def check_arbitrage(market1: Dict, market2: Dict) -> Optional[Dict]:
    """
    Check for arbitrage opportunities between two prediction markets.

    Args:
        market1: First market data (Kalshi-style format).
        market2: Second market data (Polymarket-style format).

    Returns:
        Dictionary containing arbitrage opportunity details if found, None otherwise.
    """
    try:
        # Safely extract prices from Market 1 (Kalshi)
        m1_yes_bid = market1.get('yes_bid')
        m1_yes_ask = market1.get('yes_ask')
        m1_no_bid = 100 - m1_yes_ask if m1_yes_ask is not None else None
        m1_no_ask = 100 - m1_yes_bid if m1_yes_bid is not None else None

        # Safely extract prices from Market 2 (Polymarket)
        m2_yes_ask = market2.get('bestAsk')  # Prices are in decimal form (0-1).
        m2_yes_bid = market2.get('bestBid')

        arbitrage_opportunities = []

        # Yes in Market1 vs No in Market2
        if m1_yes_bid is not None and m2_yes_ask is not None:
            if m1_yes_bid > m2_yes_ask * 100:
                arbitrage_opportunities.append({
                    'type': 'Yes-No',
                    'buy_market': 'Polymarket',
                    'sell_market': 'Kalshi',
                    'buy_price': m2_yes_ask,
                    'sell_price': m1_yes_bid / 100,
                    'profit_percentage': m1_yes_bid - (m2_yes_ask * 100)
                })

        # No in Market1 vs Yes in Market2
        if m1_no_bid is not None and m2_yes_ask is not None:
            if m1_no_bid > (1 - m2_yes_ask) * 100:
                arbitrage_opportunities.append({
                    'type': 'No-Yes',
                    'buy_market': 'Polymarket',
                    'sell_market': 'Kalshi',
                    'buy_price': m2_yes_ask,
                    'sell_price': m1_no_bid / 100,
                    'profit_percentage': m1_no_bid - ((1 - m2_yes_ask) * 100)
                })

        # Return the most profitable arbitrage opportunity, if any
        if arbitrage_opportunities:
            return max(arbitrage_opportunities, key=lambda x: x['profit_percentage'])

        return None  # No arbitrage opportunities found.

    except Exception as e:
        raise ValueError(f"Error checking arbitrage: {e}")



def find_arbitrage(similarity_threshold: int = 75):
    # Fetch Kalshi and Polymarket events
    kalshi_events = get_kalshi_events()  # Fetch Kalshi events
    polymarket_events = get_polymarket_events()  # Fetch Polymarket events

    # Check if either dataset is empty
    if kalshi_events.is_empty() or polymarket_events.is_empty():
        print("One or both datasets are empty. No arbitrage opportunities.")
        return

    # Find event matches based on similarity
    matches = compare_events(similarity_threshold=similarity_threshold)

    print("\nChecking for arbitrage opportunities...\n")
    for match in matches:
        kalshi_title = match['kalshi_title']
        poly_title = match['poly_title']
        kalshi_index = match['kalshi_index']
        poly_index = match['poly_index']

        # Safely access Kalshi and Polymarket market data using indices
        kalshi_market = kalshi_events.to_dicts()[kalshi_index]  # Convert row to dictionary
        poly_market = polymarket_events.to_dicts()[poly_index]  # Convert row to dictionary

        # Check for arbitrage
        arbitrage = check_arbitrage(kalshi_market, poly_market)
        if arbitrage:
            print(f"Arbitrage Opportunity Found!")
            print(f"Match: {kalshi_title} vs {poly_title}")
            print(f"Type: {arbitrage['type']}")
            print(f"Buy from: {arbitrage['buy_market']} at {arbitrage['buy_price']}")
            print(f"Sell on: {arbitrage['sell_market']} at {arbitrage['sell_price']}")
            print(f"Profit Percentage: {arbitrage['profit_percentage']}%\n")
        else:
            print(f"No arbitrage opportunity for match: {kalshi_title} vs {poly_title}")




# Example usage
if __name__ == "__main__":
    # get_kalshi_events()
    find_arbitrage(similarity_threshold=80)
