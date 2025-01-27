<h1 align="center">Welcome To Prediction Markets.git</h1>

This is a continuation of a [**friend of mine's**](https://github.com/carterbassler) project. The ultimate end-goal is to **fully automate arbitrage hedging**, making it function like a growing stock portfolio where you can keep adding funds and watch it grow.

This project utilizes the APIs of two prediction market services – **Kalshi** and **Polymarket**. Using the [**fuzzywuzzy string-similarity library**](https://pypi.org/project/fuzzywuzzy/) 🧠, the code identifies similar markets on both platforms and flags opportunities where the odds differ significantly.

### Example Market Opportunity
Say there is a market: **“Do the Eagles Win the Super Bowl?”** 
- On **Polymarket**, the "Yes" odds are 0.59  
- On **Kalshi**, the "No" odds are 0.54  

By hedging your bets across these two platforms' differing markets, you **guarantee** a profit.

---

### Current Progress
The project is currently in **Stage 1**:
- API responses are handled.
- Outputs from each website are formatted to match.  
- The main algorithm identifies viable arbitrage opportunities.

---

### Next Steps
1. Web integration --> Automating live placement of arbitrage opportunities.  
2. Further research to optimize it.  

