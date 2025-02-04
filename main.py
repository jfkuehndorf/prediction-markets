from polymarket import get_polymarket_events
from kalshi import get_kalshi_events
from rapidfuzz import fuzz
import polars as pl
from datetime import datetime, timedelta

def find_arbitrage(similarity_threshold: int = 75, min_profit: float = 2.0, max_days_left: int = 5, stake: float = 100):
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

                if kalshi_market.get("endDate") is None or poly_market.get("endDate") is None:
                    continue

                end_date = min(kalshi_market.get("endDate"),poly_market.get("endDate"))
                print(f"End Date: {end_date}")
                event_time_remaining = (end_date - datetime.today()).days
                print(f"Event_time_remaining: {event_time_remaining} days")
                if event_time_remaining > max_days_left:
                    print(f"Skipped, too many days ({event_time_remaining}) remaining of market close.")
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
                            k_no_ask = 100 - k_yes_ask if k_yes_ask is not None else None

                            p_yes_ask = next((p_outcome[key] for key in p_outcome.keys() if isinstance(p_outcome[key], (int, float))), None)
                            p_no_ask = 100 - p_yes_ask if p_yes_ask is not None else None
                            
                            profit = None
                            arbitrage_percentage = None

                            # CHECKING ARBITRAGE
                            if k_yes_ask is not None and p_no_ask is not None and k_yes_ask < p_no_ask:
                                yes_stake = stake / (1 + (p_no_ask / k_yes_ask))
                                print(f"Yes Stake: {yes_stake}")
                                no_stake = stake - yes_stake
                                print(f"No Stake: {no_stake}")
                                total_cost = yes_stake + no_stake
                                print("Calculating Arbitrage Opportunities...")
                                yes_payout = (yes_stake / (k_yes_ask / 100))  # Total return if YES wins
                                print(f"Yes Payout: {yes_payout}")
                                no_payout = (no_stake / (p_no_ask / 100))  # Total return if NO wins
                                print(f"No Payout: {no_payout}")

                                profit_yes_wins = yes_payout - total_cost
                                print(f"Profit if YES wins: {profit_yes_wins}")
                                profit_no_wins = no_payout - total_cost
                                print(f"Profit if NO wins: {profit_no_wins}")
                                print("")
                                if min(profit_yes_wins, profit_no_wins) > -0.01 * stake:  # Allows very close risk-free trades
                                    profit = min(profit_yes_wins, profit_no_wins)
                                    arbitrage_percentage = (profit / total_cost) * 100
                                else:
                                    profit = -1
                                    arbitrage_percentage = -1

                                arbitrage_percentage = (profit / total_cost) * 100 if profit is not -1 else -1

                                if profit >= min_profit:
                                    print(f"\n** Arbitrage Opportunity Found! **")
                                    print(f"Event: {kalshi_title} / {poly_title}")
                                    print(f"** Matched Outcome: {k_outcome[k_label]} <-> {p_outcome[p_label]} (Similarity: {outcome_similarity}%)")
                                    print(f"✅ Buy YES on Kalshi at {k_yes_ask}% (Stake ${yes_stake:.2f})")
                                    print(f"🚫 Buy NO on Polymarket at {p_no_ask}% (Stake ${no_stake:.2f})")
                                    print(f"** Expected Profit: ${profit:.2f} Arbitrage Percentage: ({arbitrage_percentage:.2f}%) **")
                                    print(f"Time Remaining: {event_time_remaining} days")
                                    print("-" * 50)

                            elif p_yes_ask is not None and k_no_ask is not None and p_yes_ask < k_no_ask:
                                yes_stake = stake / (1 + (p_no_ask / k_yes_ask))
                                no_stake = stake - yes_stake

                                total_cost = yes_stake + no_stake

                                yes_payout = (yes_stake / (k_yes_ask / 100))  # Total return if YES wins
                                no_payout = (no_stake / (p_no_ask / 100))  # Total return if NO wins

                                profit_yes_wins = yes_payout - total_cost
                                profit_no_wins = no_payout - total_cost

                                if min(profit_yes_wins, profit_no_wins) > -0.01 * stake:  # Allows very close risk-free trades
                                    profit = min(profit_yes_wins, profit_no_wins)
                                    arbitrage_percentage = (profit / total_cost) * 100
                                else:
                                    profit = -1
                                    arbitrage_percentage = -1

                                arbitrage_percentage = (profit / total_cost) * 100 if profit is not -1 else -1

                                if profit >= min_profit:
                                    print(f"\n** Arbitrage Opportunity Found! **")
                                    print(f"Event: {kalshi_title} / {poly_title}")
                                    print(f"** Matched Outcome: {k_outcome[k_label]} <-> {p_outcome[p_label]} (Similarity: {outcome_similarity}%)")
                                    print(f"✅ Buy YES on Polymarket at {p_yes_ask}% (Stake ${yes_stake:.2f})")
                                    print(f"🚫 Buy NO on Kalshi at {k_no_ask}% (Stake ${no_stake:.2f})")
                                    print(f"** Expected Profit: ${profit:.2f} Arbitrage Percentage: ({arbitrage_percentage:.2f}%) **")
                                    print(f"Time Remaining: {event_time_remaining} days")
                                    print("-" * 50)

                            matches.append({
                                "kalshi_title": kalshi_title,
                                "poly_title": poly_title,
                                "matched_outcome": k_outcome[k_label],
                                "event_similarity": similarity_score,
                                "outcome_similarity": outcome_similarity,
                                "kalshi_yes_ask": k_yes_ask,
                                "kalshi_no_ask": k_no_ask,
                                "polymarket_yes_ask": p_yes_ask,
                                "polymarket_no_ask": p_no_ask,
                                "profit": profit,
                                "arbitrage_percentage": arbitrage_percentage,
                                "time_remaining": event_time_remaining
                            })

    if not matches:
        print("No arbitrage opportunities found.")

if __name__ == "__main__":
    find_arbitrage(similarity_threshold=80, min_profit=1.0, max_days_left=5,stake=100)
 