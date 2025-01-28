from polymarket import get_polymarket_events
from kalshi import get_kalshi_events
from rapidfuzz import fuzz
import polars as pl

def find_arbitrage(similarity_threshold: int = 75, min_profit: float = 2.0, min_time: int = 0):
    """Find arbitrage opportunities by comparing Kalshi & Polymarket event lines."""

    # Fetch events from both sources
    kalshi_events = get_kalshi_events()
    polymarket_events = get_polymarket_events()

    if kalshi_events.is_empty() or polymarket_events.is_empty():
        print("No events found in one or both datasets.")
        return

    if 'title' not in kalshi_events.columns or 'title' not in polymarket_events.columns:
        raise ValueError("Missing 'title' column in one of the datasets.")

    kalshi_titles = kalshi_events['title'].to_list()
    poly_titles = polymarket_events['title'].to_list()

    matches = []
    for k_idx, kalshi_title in enumerate(kalshi_titles):
        for p_idx, poly_title in enumerate(poly_titles):
            similarity_score = fuzz.ratio(kalshi_title.lower(), poly_title.lower())

            if similarity_score >= similarity_threshold:
                kalshi_market = kalshi_events.to_dicts()[k_idx]
                poly_market = polymarket_events.to_dicts()[p_idx]

                kalshi_outcomes = kalshi_market.get("outcomes", [])
                poly_outcomes = poly_market.get("outcomes", [])

                if not isinstance(kalshi_outcomes, list) or not isinstance(poly_outcomes, list):
                    continue

                event_time_remaining = kalshi_market.get("time_remaining", 9999)
                if event_time_remaining < min_time:
                    continue

                for k_outcome in kalshi_outcomes:
                    for p_outcome in poly_outcomes:
                        k_label = next((k for k in k_outcome.keys() if isinstance(k_outcome[k], str)), None)
                        p_label = next((p for p in p_outcome.keys() if isinstance(p_outcome[p], str)), None)

                        if not k_label or not p_label:
                            continue

                        outcome_similarity = fuzz.ratio(k_outcome[k_label].lower(), p_outcome[p_label].lower())

                        if outcome_similarity >= similarity_threshold:
                            k_yes_ask = next((k_outcome[key] for key in k_outcome.keys() if isinstance(k_outcome[key], (int, float))), None)
                            k_no_bid = 100 - k_yes_ask if k_yes_ask is not None else None

                            p_yes_ask = next((p_outcome[key] for key in p_outcome.keys() if isinstance(p_outcome[key], (int, float))), None)
                            p_no_bid = 100 - p_yes_ask if p_yes_ask is not None else None

                            # Check arbitrage (YES on one site, NO on the other)
                            if k_yes_ask is not None and p_no_bid is not None and k_yes_ask < p_no_bid:
                                yes_stake = 100 / k_yes_ask  # Stake needed to win $100 on YES
                                no_stake = 100 / p_no_bid    # Stake needed to win $100 on NO
                                total_cost = yes_stake + no_stake
                                profit = 100 - total_cost
                                profit_percentage = (profit / total_cost) * 100

                                if profit_percentage >= min_profit:
                                    print(f"\n** Arbitrage Opportunity Found! **")
                                    print(f"Event: {kalshi_title} / {poly_title}")
                                    print(f"** Matched Outcome: {k_outcome[k_label]} <-> {p_outcome[p_label]} (Similarity: {outcome_similarity}%)")
                                    print(f"✅ Buy YES on Kalshi at {k_yes_ask}% (Stake ${yes_stake:.2f})")
                                    print(f"🚫 Buy NO on Polymarket at {p_no_bid}% (Stake ${no_stake:.2f})")
                                    print(f"** Expected Profit: ${profit:.2f} ({profit_percentage:.2f}%) **")
                                    print("-" * 50)

                            elif p_yes_ask is not None and k_no_bid is not None and p_yes_ask < k_no_bid:
                                yes_stake = 100 / p_yes_ask  # Stake needed to win $100 on YES
                                no_stake = 100 / k_no_bid    # Stake needed to win $100 on NO
                                total_cost = yes_stake + no_stake
                                profit = 100 - total_cost
                                profit_percentage = (profit / total_cost) * 100

                                if profit_percentage >= min_profit:
                                    print(f"\n** Arbitrage Opportunity Found! **")
                                    print(f"Event: {kalshi_title} / {poly_title}")
                                    print(f"** Matched Outcome: {k_outcome[k_label]} <-> {p_outcome[p_label]} (Similarity: {outcome_similarity}%)")
                                    print(f"✅ Buy YES on Polymarket at {p_yes_ask}% (Stake ${yes_stake:.2f})")
                                    print(f"🚫 Buy NO on Kalshi at {k_no_bid}% (Stake ${no_stake:.2f})")
                                    print(f"** Expected Profit: ${profit:.2f} ({profit_percentage:.2f}%) **")
                                    print("-" * 50)

                            matches.append({
                                "kalshi_title": kalshi_title,
                                "poly_title": poly_title,
                                "matched_outcome": k_outcome[k_label],
                                "event_similarity": similarity_score,
                                "outcome_similarity": outcome_similarity,
                                "kalshi_yes_ask": k_yes_ask,
                                "kalshi_no_bid": k_no_bid,
                                "polymarket_yes_ask": p_yes_ask,
                                "polymarket_no_bid": p_no_bid,
                                "profit": profit,
                                "profit_percentage": profit_percentage,
                                "time_remaining": event_time_remaining
                            })

    if not matches:
        print("No arbitrage opportunities found.")

if __name__ == "__main__":
    find_arbitrage(similarity_threshold=80, min_profit=3.0, min_time=24)
