# LLM + Tool Call Sequence

1) User provides question in natural language.
2) Planner LLM call returns structured intent JSON.
3) Generator LLM call returns parameterized SQL template.
4) Validator checks:
   - SELECT only
   - no dangerous keywords (DROP/DELETE/UPDATE/INSERT)
   - table whitelist
   - LIMIT caps
5) Executor runs query with SQLAlchemy.
6) If time-series/categorical → plot created with Matplotlib.
7) Return output to user.