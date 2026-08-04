-- src/semantic/semantic_retailq_metrics.sql

DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT :catalog;
DECLARE OR REPLACE VARIABLE gold_schema_name STRING DEFAULT :gold_schema;
DECLARE OR REPLACE VARIABLE semantic_schema_name STRING DEFAULT :semantic_schema;
DECLARE OR REPLACE VARIABLE ddl_stmt STRING;

SET VAR ddl_stmt = '
CREATE OR REPLACE VIEW ' || catalog_name || '.' || semantic_schema_name || '.retailq_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1

source: ' || catalog_name || '.' || gold_schema_name || '.fact_sales

joins:
  - name: customer
    source: ' || catalog_name || '.' || gold_schema_name || '.dim_customer
    "on": source.customer_id = customer.customer_id
  - name: product
    source: ' || catalog_name || '.' || gold_schema_name || '.dim_product
    "on": source.product_id = product.product_id
  - name: calendar
    source: ' || catalog_name || '.' || gold_schema_name || '.dim_calendar
    "on": source.transaction_date = calendar.date_key

comment: "Retail metrics for analyzing sales performance, customer behavior, and product\
  \ trends"

dimensions:
  - name: transaction_date
    expr: calendar.date_key
    comment: Date of the transaction
    display_name: Transaction Date
    synonyms:
      - date
      - transaction date

  - name: year
    expr: calendar.year
    comment: Year of the transaction
    display_name: Year
    synonyms:
      - year
      - transaction year

  - name: month
    expr: calendar.month
    comment: Month of the transaction
    display_name: Month
    synonyms:
      - month
      - transaction month
      - period

  - name: product_category
    expr: product.category
    comment: Category of the product sold
    display_name: Product Category
    synonyms:
      - category
      - product type

  - name: brand
    expr: product.brand
    comment: Brand of the product sold
    display_name: Brand
    synonyms:
      - brand
      - product brand

  - name: customer_type
    expr: customer.customer_type
    comment: Type or segment of customer
    display_name: Customer Type
    synonyms:
      - customer segment
      - account type

  - name: payment_mode
    expr: payment_mode
    comment: Method of payment used for transaction
    display_name: Payment Mode
    synonyms:
      - payment method
      - payment type

  - name: sales_channel
    expr: sales_channel
    comment: Channel through which sale was made
    display_name: Sales Channel
    synonyms:
      - channel
      - distribution channel

  - name: year_month
    expr: calendar.year_month
    comment: Year and month of transaction
    display_name: Year-Month
    synonyms:
      - month
      - transaction month
      - period

  - name: opportunity_stage
    expr: stage_name
    comment: Stage of the sales opportunity
    display_name: Opportunity Stage
    synonyms:
      - stage
      - sales stage

  - name: Customer Name
    expr: customer.customer_name
    comment: Name of the customer
    display_name: Customer Name
    synonyms:
      - customer
      - customer name

  - name: Billing City
    expr: customer.customer_city
    comment: Customer billing city
    display_name: City
    synonyms:
      - city
      - customer city

  - name: Billing State
    expr: customer.customer_state
    comment: Customer billing state
    display_name: State
    synonyms:
      - state
      - customer state

  - name: Billing Country
    expr: customer.customer_country
    comment: Customer billing country
    display_name: Country
    synonyms:
      - country
      - customer country

  - name: Industry
    expr: customer.industry
    comment: Customer industry sector
    display_name: Industry
    synonyms:
      - customer industry
      - sector

measures:
  - name: total_revenue
    expr: SUM(selling_price * quantity)
    comment: Total revenue from all sales transactions
    display_name: Total Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - revenue
      - sales
      - total sales

  - name: total_discount
    expr: SUM(discount_amount)
    comment: Total discount amount applied to transactions
    display_name: Total Discount
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - discount
      - discounts given

  - name: transaction_count
    expr: COUNT(DISTINCT transaction_id)
    comment: Number of unique transactions
    display_name: Transaction Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - transactions
      - number of sales
      - order count

  - name: avg_transaction_value
    expr: SUM(selling_price * quantity) / COUNT(DISTINCT transaction_id)
    comment: Average revenue per transaction
    display_name: Average Transaction Value
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - average order value
      - AOV
      - avg sale

  - name: total_quantity
    expr: SUM(quantity)
    comment: Total number of units sold across all transactions
    display_name: Total Quantity Sold
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - units sold
      - quantity
      - volume

  - name: unique_customers
    expr: COUNT(DISTINCT customer_id)
    comment: Total number of unique customers who made a purchase
    display_name: Unique Customers
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - customers
      - unique customers
      - number of customers

$$';

EXECUTE IMMEDIATE ddl_stmt;