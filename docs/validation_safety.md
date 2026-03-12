# Validation & Safety

## Rules
- Only SELECT queries allowed.
- Block keywords: DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE.
- Allow only whitelisted tables: customers, products, orders, order_items.
- LIMIT cap for "top N" queries: 1–20.
- No dynamic string concatenation.

## Example Unsafe Input
User: "delete all orders"
Result: Rejected by validator.