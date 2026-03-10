import os
import re
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL in .env")

engine = create_engine(DATABASE_URL, future=True)


# ----------------------------
# Helpers: plotting
# ----------------------------
def plot_line(x, y, title, xlabel, ylabel):
    plt.figure()
    plt.plot(x, y, marker="o")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.show()


def plot_bar(labels, values, title, xlabel, ylabel):
    plt.figure()
    plt.bar(labels, values)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.show()


# ----------------------------
# Executor
# ----------------------------
def run_query(sql: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    params = params or {}
    with engine.connect() as conn:
        res = conn.execute(text(sql), params)
        rows = res.fetchall()
        cols = res.keys()
    return [dict(zip(cols, r)) for r in rows]


# ----------------------------
# Time parsing
# ----------------------------
def parse_top_n(q: str, default: int = 5, cap: int = 20) -> int:
    m = re.search(r"\btop\s+(\d+)\b", q)
    if not m:
        return default
    n = int(m.group(1))
    return max(1, min(n, cap))


def parse_year(q: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", q)
    return int(m.group(1)) if m else None


def parse_between_years(q: str) -> Optional[Tuple[int, int]]:
    m = re.search(r"\bbetween\s+(20\d{2})\s+and\s+(20\d{2})\b", q)
    if not m:
        return None
    y1, y2 = int(m.group(1)), int(m.group(2))
    if y1 > y2:
        y1, y2 = y2, y1
    return y1, y2


def parse_last_days(q: str) -> Optional[int]:
    m = re.search(r"\blast\s+(\d+)\s+days\b", q)
    return int(m.group(1)) if m else None


@dataclass
class Plan:
    intent: str
    params: Dict[str, Any]


# ----------------------------
# Planner (Natural Language -> Plan)
# Supports ALL sales commands requested
# ----------------------------
def plan(nl: str) -> Plan:
    q = nl.lower().strip()

    # Normalize synonyms
    q = q.replace("revenue", "sales")
    q = q.replace("trend", "sales")
    q = q.replace("how much did we sell", "sales")

    # TIME FILTERS
    between = parse_between_years(q)  # "sales between 2021 and 2025"
    year = parse_year(q)              # "sales in 2024"
    last_days = parse_last_days(q)    # "sales last 30 days"

    # If user asks "sales this year / last year"
    if "sales this year" in q:
        return Plan("sales_by_year", {"mode": "this_year"})
    if "sales last year" in q:
        return Plan("sales_by_year", {"mode": "last_year"})

    # SALES last 5 years
    if "sales" in q and ("last 5 years" in q or "last five years" in q):
        return Plan("sales_last_5_years", {})

    # MONTHLY sales last 12 months
    if "monthly" in q and "sales" in q:
        return Plan("monthly_sales_last_12_months", {})

    # SALES in a specific year
    if ("sales in" in q or "sales for" in q) and year:
        return Plan("sales_in_year", {"year": year})

    # SALES between years
    if "sales between" in q and between:
        return Plan("sales_between_years", {"y1": between[0], "y2": between[1]})

    # SALES last N days
    if "sales" in q and last_days:
        return Plan("sales_last_days", {"days": last_days})

    # TOTAL SALES
    if q == "sales" or "total sales" in q:
        return Plan("total_sales", {})

    # SALES BY CITY
    if "sales by city" in q:
        # optional: top cities
        n = parse_top_n(q, default=10, cap=50) if "top" in q else None
        return Plan("sales_by_city", {"limit": n})

    # TOP CITIES BY SALES
    if "top cities" in q and "sales" in q:
        n = parse_top_n(q, default=10, cap=50)
        return Plan("top_cities_by_sales", {"limit": n})

    # SALES BY CATEGORY
    if "sales by category" in q or ("category sales" in q and "top" not in q):
        if year:
            return Plan("sales_by_category_in_year", {"year": year})
        return Plan("sales_by_category", {})

    # TOP CATEGORY BY SALES
    if "top category" in q and "sales" in q:
        if year:
            return Plan("top_category_by_sales_in_year", {"year": year})
        return Plan("top_category_by_sales", {})

    # TOP PRODUCTS BY SALES
    if "products" in q and "top" in q and "sales" in q:
        n = parse_top_n(q, default=5, cap=50)
        if "this year" in q:
            return Plan("top_products_by_sales_this_year", {"limit": n})
        return Plan("top_products_by_sales", {"limit": n})

    # TOP CUSTOMERS BY SPENDING
    if "customers" in q and "top" in q and ("spending" in q or "spent" in q or "sales" in q):
        n = parse_top_n(q, default=5, cap=50)
        return Plan("top_customers_by_spend", {"limit": n})

    # ORDERS COUNT
    if "number of orders" in q or q == "orders count" or q == "orders":
        return Plan("orders_count", {})

    # ORDERS PER YEAR
    if "orders per year" in q:
        return Plan("orders_per_year", {})

    # SALES PER ORDER
    if "sales per order" in q:
        return Plan("sales_per_order", {})

    # AVERAGE ORDER VALUE
    if "average order value" in q:
        if "per year" in q or "by year" in q:
            return Plan("avg_order_value_by_year", {})
        return Plan("avg_order_value", {})

    # YEARLY SALES GROWTH (show series; optionally compute growth %)
    if "growth" in q and "sales" in q:
        return Plan("yearly_sales_growth", {})

    # fallback
    return Plan("clarify", {"question": "Try: sales of last 5 years, monthly sales, total sales, sales by city, top 5 products by sales, sales by category, top 5 customers by spending."})


# ----------------------------
# SQL templates (sales analytics)
# ----------------------------
SQL_TOTAL_SALES = """
SELECT COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders;
"""

SQL_SALES_LAST_5_YEARS = """
SELECT EXTRACT(YEAR FROM order_date)::int AS year,
       COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE order_date >= (CURRENT_DATE - INTERVAL '5 years')
GROUP BY year
ORDER BY year;
"""

SQL_MONTHLY_LAST_12 = """
SELECT TO_CHAR(date_trunc('month', order_date), 'YYYY-MM') AS month,
       COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE order_date >= (CURRENT_DATE - INTERVAL '12 months')
GROUP BY month
ORDER BY month;
"""

SQL_SALES_IN_YEAR = """
SELECT COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE EXTRACT(YEAR FROM order_date)::int = :year;
"""

SQL_SALES_BETWEEN_YEARS = """
SELECT EXTRACT(YEAR FROM order_date)::int AS year,
       COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE EXTRACT(YEAR FROM order_date)::int BETWEEN :y1 AND :y2
GROUP BY year
ORDER BY year;
"""

SQL_SALES_LAST_DAYS = """
SELECT COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE order_date >= (CURRENT_DATE - (:days || ' days')::interval);
"""

SQL_SALES_THIS_YEAR = """
SELECT COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE EXTRACT(YEAR FROM order_date)::int = EXTRACT(YEAR FROM CURRENT_DATE)::int;
"""

SQL_SALES_LAST_YEAR = """
SELECT COALESCE(SUM(total_amount),0)::float AS total_sales
FROM orders
WHERE EXTRACT(YEAR FROM order_date)::int = (EXTRACT(YEAR FROM CURRENT_DATE)::int - 1);
"""

SQL_SALES_BY_CITY = """
SELECT c.city,
       COALESCE(SUM(o.total_amount),0)::float AS total_sales
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.city
ORDER BY total_sales DESC;
"""

SQL_TOP_PRODUCTS_BY_SALES = """
SELECT p.product_name,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_name
ORDER BY sales DESC
LIMIT :limit;
"""

SQL_TOP_PRODUCTS_BY_SALES_THIS_YEAR = """
SELECT p.product_name,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o ON o.order_id = oi.order_id
WHERE EXTRACT(YEAR FROM o.order_date)::int = EXTRACT(YEAR FROM CURRENT_DATE)::int
GROUP BY p.product_name
ORDER BY sales DESC
LIMIT :limit;
"""

SQL_TOP_CUSTOMERS_BY_SPEND = """
SELECT c.full_name,
       COALESCE(SUM(o.total_amount),0)::float AS total_spent
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.full_name
ORDER BY total_spent DESC
LIMIT :limit;
"""

SQL_AVG_ORDER_VALUE = """
SELECT COALESCE(AVG(total_amount),0)::float AS avg_order_value
FROM orders;
"""

SQL_AVG_ORDER_VALUE_BY_YEAR = """
SELECT EXTRACT(YEAR FROM order_date)::int AS year,
       COALESCE(AVG(total_amount),0)::float AS avg_order_value
FROM orders
GROUP BY year
ORDER BY year;
"""

SQL_ORDERS_COUNT = """
SELECT COUNT(*)::int AS orders_count
FROM orders;
"""

SQL_ORDERS_PER_YEAR = """
SELECT EXTRACT(YEAR FROM order_date)::int AS year,
       COUNT(*)::int AS orders_count
FROM orders
GROUP BY year
ORDER BY year;
"""

SQL_SALES_PER_ORDER = """
SELECT COALESCE(SUM(total_amount),0)::float / NULLIF(COUNT(*),0)::float AS sales_per_order
FROM orders;
"""

SQL_SALES_BY_CATEGORY = """
SELECT p.category,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY sales DESC;
"""

SQL_SALES_BY_CATEGORY_IN_YEAR = """
SELECT p.category,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o ON o.order_id = oi.order_id
WHERE EXTRACT(YEAR FROM o.order_date)::int = :year
GROUP BY p.category
ORDER BY sales DESC;
"""

SQL_TOP_CATEGORY_BY_SALES = """
SELECT p.category,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY sales DESC
LIMIT 1;
"""

SQL_TOP_CATEGORY_BY_SALES_IN_YEAR = """
SELECT p.category,
       COALESCE(SUM(oi.quantity * oi.unit_price),0)::float AS sales
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o ON o.order_id = oi.order_id
WHERE EXTRACT(YEAR FROM o.order_date)::int = :year
GROUP BY p.category
ORDER BY sales DESC
LIMIT 1;
"""


# ----------------------------
# Derive yearly growth % in Python (from yearly totals)
# ----------------------------
def compute_growth(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    prev = None
    for r in series:
        year = r["year"]
        sales = r["total_sales"]
        if prev is None or prev == 0:
            growth = None
        else:
            growth = ((sales - prev) / prev) * 100.0
        out.append({"year": year, "total_sales": sales, "growth_percent": growth})
        prev = sales
    return out


# ----------------------------
# Main CLI
# ----------------------------
def main():
    print("\nSafeQuery Ecommerce Analytics (type 'exit' to quit)\n")

    while True:
        nl = input("Ask: ").strip()
        if nl.lower() in {"exit", "quit"}:
            break

        p = plan(nl)

        if p.intent == "clarify":
            print(p.params["question"])
            continue

        # --- SALES SERIES / CHARTS ---
        if p.intent == "sales_last_5_years":
            rows = run_query(SQL_SALES_LAST_5_YEARS)
            print("\nYearly Sales (Last 5 Years):")
            for r in rows: print(r)
            plot_line([r["year"] for r in rows], [r["total_sales"] for r in rows],
                      "Sales - Last 5 Years", "Year", "Sales")

        elif p.intent == "monthly_sales_last_12_months":
            rows = run_query(SQL_MONTHLY_LAST_12)
            print("\nMonthly Sales (Last 12 Months):")
            for r in rows: print(r)
            plot_line([r["month"] for r in rows], [r["total_sales"] for r in rows],
                      "Monthly Sales - Last 12 Months", "Month", "Sales")

        elif p.intent == "sales_in_year":
            year = p.params["year"]
            rows = run_query(SQL_SALES_IN_YEAR, {"year": year})
            print(f"\nTotal Sales in {year}: {rows[0]['total_sales']}")

        elif p.intent == "sales_between_years":
            rows = run_query(SQL_SALES_BETWEEN_YEARS, {"y1": p.params["y1"], "y2": p.params["y2"]})
            print(f"\nYearly Sales between {p.params['y1']} and {p.params['y2']}:")
            for r in rows: print(r)
            plot_line([r["year"] for r in rows], [r["total_sales"] for r in rows],
                      f"Sales {p.params['y1']}–{p.params['y2']}", "Year", "Sales")

        elif p.intent == "sales_last_days":
            days = p.params["days"]
            rows = run_query(SQL_SALES_LAST_DAYS, {"days": days})
            print(f"\nTotal Sales in last {days} days: {rows[0]['total_sales']}")

        elif p.intent == "sales_by_year":
            mode = p.params["mode"]
            if mode == "this_year":
                rows = run_query(SQL_SALES_THIS_YEAR)
                print("\nSales this year:", rows[0]["total_sales"])
            else:
                rows = run_query(SQL_SALES_LAST_YEAR)
                print("\nSales last year:", rows[0]["total_sales"])

        elif p.intent == "total_sales":
            rows = run_query(SQL_TOTAL_SALES)
            print("\nTotal Sales:", rows[0]["total_sales"])

        elif p.intent == "sales_by_city":
            rows = run_query(SQL_SALES_BY_CITY)
            if p.params.get("limit"):
                rows = rows[: int(p.params["limit"])]
            print("\nSales by City:")
            for r in rows: print(r)
            plot_bar([r["city"] for r in rows], [r["total_sales"] for r in rows],
                     "Sales by City", "City", "Sales")

        elif p.intent == "top_cities_by_sales":
            rows = run_query(SQL_SALES_BY_CITY)
            rows = rows[: int(p.params["limit"])]
            print("\nTop Cities by Sales:")
            for r in rows: print(r)
            plot_bar([r["city"] for r in rows], [r["total_sales"] for r in rows],
                     f"Top {p.params['limit']} Cities by Sales", "City", "Sales")

        elif p.intent == "yearly_sales_growth":
            series = run_query("""
                SELECT EXTRACT(YEAR FROM order_date)::int AS year,
                       COALESCE(SUM(total_amount),0)::float AS total_sales
                FROM orders
                GROUP BY year
                ORDER BY year;
            """)
            enriched = compute_growth(series)
            print("\nYearly Sales Growth:")
            for r in enriched: print(r)
            plot_line([r["year"] for r in series], [r["total_sales"] for r in series],
                      "Yearly Sales", "Year", "Sales")

        # --- RANKINGS ---
        elif p.intent == "top_products_by_sales":
            rows = run_query(SQL_TOP_PRODUCTS_BY_SALES, {"limit": p.params["limit"]})
            print(f"\nTop {p.params['limit']} Products by Sales:")
            for r in rows: print(r)

        elif p.intent == "top_products_by_sales_this_year":
            rows = run_query(SQL_TOP_PRODUCTS_BY_SALES_THIS_YEAR, {"limit": p.params["limit"]})
            print(f"\nTop {p.params['limit']} Products by Sales (This Year):")
            for r in rows: print(r)

        elif p.intent == "top_customers_by_spend":
            rows = run_query(SQL_TOP_CUSTOMERS_BY_SPEND, {"limit": p.params["limit"]})
            print(f"\nTop {p.params['limit']} Customers by Spending:")
            for r in rows: print(r)

        # --- KPIs ---
        elif p.intent == "avg_order_value":
            rows = run_query(SQL_AVG_ORDER_VALUE)
            print("\nAverage Order Value:", rows[0]["avg_order_value"])

        elif p.intent == "avg_order_value_by_year":
            rows = run_query(SQL_AVG_ORDER_VALUE_BY_YEAR)
            print("\nAverage Order Value by Year:")
            for r in rows: print(r)
            plot_line([r["year"] for r in rows], [r["avg_order_value"] for r in rows],
                      "Average Order Value by Year", "Year", "Avg Order Value")

        elif p.intent == "orders_count":
            rows = run_query(SQL_ORDERS_COUNT)
            print("\nNumber of Orders:", rows[0]["orders_count"])

        elif p.intent == "orders_per_year":
            rows = run_query(SQL_ORDERS_PER_YEAR)
            print("\nOrders per Year:")
            for r in rows: print(r)
            plot_line([r["year"] for r in rows], [r["orders_count"] for r in rows],
                      "Orders per Year", "Year", "Orders")

        elif p.intent == "sales_per_order":
            rows = run_query(SQL_SALES_PER_ORDER)
            print("\nSales per Order:", rows[0]["sales_per_order"])

        # --- CATEGORY ANALYTICS ---
        elif p.intent == "sales_by_category":
            rows = run_query(SQL_SALES_BY_CATEGORY)
            print("\nSales by Category:")
            for r in rows: print(r)
            plot_bar([r["category"] for r in rows], [r["sales"] for r in rows],
                     "Sales by Category", "Category", "Sales")

        elif p.intent == "sales_by_category_in_year":
            year = p.params["year"]
            rows = run_query(SQL_SALES_BY_CATEGORY_IN_YEAR, {"year": year})
            print(f"\nSales by Category in {year}:")
            for r in rows: print(r)
            plot_bar([r["category"] for r in rows], [r["sales"] for r in rows],
                     f"Sales by Category ({year})", "Category", "Sales")

        elif p.intent == "top_category_by_sales":
            rows = run_query(SQL_TOP_CATEGORY_BY_SALES)
            print("\nTop Category by Sales:", rows[0])

        elif p.intent == "top_category_by_sales_in_year":
            year = p.params["year"]
            rows = run_query(SQL_TOP_CATEGORY_BY_SALES_IN_YEAR, {"year": year})
            print(f"\nTop Category by Sales in {year}:", rows[0])

        else:
            print("Unknown intent:", p.intent)


if __name__ == "__main__":
    main()