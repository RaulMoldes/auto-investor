FILTER_SYSTEM_PROMPT = """\
You are a financial news classifier. Analyze the given article and output JSON.
Only output valid JSON, no extra text."""

FILTER_PROMPT_TEMPLATE = """\
Classify this financial article. Output JSON with these fields:
- relevance (0-10): how relevant to investment decisions
- sentiment: "bullish", "bearish", or "neutral"
- tickers: list of mentioned stock/ETF tickers
- key_facts: list of 1-3 key facts

Example output:
{{"relevance": 7, "sentiment": "bullish", "tickers": ["AAPL", "SPY"],
"key_facts": ["Apple revenue beat estimates by 5%"]}}

Article title: {title}
Article content: {content}

Output JSON:"""

ANALYSIS_SYSTEM_PROMPT = """\
You are an investment analyst. Given filtered financial articles and market data, \
produce a monthly investment recommendation. Think step by step. Output valid JSON only."""

ANALYSIS_PROMPT_TEMPLATE = """\
Based on the following data, produce a monthly investment recommendation.

## Market Data
{market_data}

## Key Articles (filtered by relevance)
{articles}

## Previous Recommendations Summary
{history}

Think step by step about:
1. Current market conditions and trends
2. Key risks and opportunities
3. Asset allocation considering risk level

Output JSON with this exact structure:
{{
  "date": "{date}",
  "market_summary": "2-3 sentence overview",
  "recommendation": {{
    "action": "BUY or HOLD or REBALANCE",
    "assets": [
      {{
        "ticker": "VWCE.DE",
        "name": "Vanguard FTSE All-World ETF",
        "allocation_pct": 60,
        "rationale": "reason for this allocation"
      }}
    ],
    "risk_level": "LOW or MEDIUM or HIGH",
    "confidence": 0.75
  }},
  "justification": "Detailed paragraph explaining reasoning",
  "key_factors": ["factor1", "factor2"],
  "risks": ["risk1", "risk2"],
  "sources_used": {sources_count}
}}

Example output:
{{
  "date": "2026-03-01",
  "market_summary": "Markets showed resilience despite inflation concerns.",
  "recommendation": {{
    "action": "BUY",
    "assets": [
      {{"ticker": "VWCE.DE", "name": "Vanguard All-World",
        "allocation_pct": 60, "rationale": "Global diversification"}},
      {{"ticker": "IUSN.DE", "name": "MSCI World Small Cap",
        "allocation_pct": 20, "rationale": "Small cap value"}},
      {{"ticker": "AGGH.DE", "name": "Global Aggregate Bond",
        "allocation_pct": 20, "rationale": "Defensive allocation"}}
    ],
    "risk_level": "MEDIUM",
    "confidence": 0.7
  }},
  "justification": "Given current conditions...",
  "key_factors": ["Strong earnings season", "ECB rate pause expected"],
  "risks": ["Geopolitical tensions", "Inflation persistence"],
  "sources_used": 25
}}

Produce your recommendation JSON:"""
