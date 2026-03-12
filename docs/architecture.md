# SafeQuery Ecommerce Agent — Architecture

## Overview
SafeQuery is a structured agentic system that converts natural-language ecommerce analytics questions into safe SQL, executes queries on PostgreSQL, and returns tables + graphs.

## Components
- **Planner (LLM Call #1)**: NL → intent JSON
- **Generator (LLM Call #2)**: intent JSON → SQL template
- **Validator (Code)**: enforces SELECT-only, table whitelist, limit caps
- **Executor (Tool)**: runs SQL via SQLAlchemy and plots via Matplotlib

## Data Source
PostgreSQL ecommerce schema: customers, products, orders, order_items.

## Output
- Table results (rows)
- Matplotlib visualization for time-series or category summaries