from pyspark import pipelines as dp
from pyspark.sql import functions as F

gold_schema = spark.conf.get("gold_schema")   # "gold"

# should be created as a Materialized View or a standard Delta Table.
@dp.materialized_view(
    name = f"{gold_schema}.dim_calendar",
    comment="Calendar dimension table with date attributes for time-based analysis"
)
def dim_calendar():
    """
    Generate a calendar dimension table with date attributes.
    Covers a 10-year range (3 years historical, 3 years future).
    """
    # Generate date range: 3 years back to 3 years forward from today  
    # Create base date sequence
    df = (spark.sql(f"""
        SELECT 
            sequence(
                to_date('2025-01-01', 'yyyy-MM-dd'),
                to_date('2028-12-31', 'yyyy-MM-dd'),
                interval 1 day
            ) as date_array
    """)
    .select(F.explode("date_array").alias("date_key"))
    )
    
    # Add calendar attributes
    calendar = df.select(
        F.col("date_key"),
        
        # Year attributes
        F.year("date_key").alias("year"),
        F.quarter("date_key").alias("quarter"),
        F.month("date_key").alias("month"),
        F.dayofmonth("date_key").alias("day"),
        
        # Week attributes
        F.weekofyear("date_key").alias("week_of_year"),
        F.dayofweek("date_key").alias("day_of_week"),
        F.date_format("date_key", "EEEE").alias("day_name"),
        
        # Month attributes
        F.date_format("date_key", "MMMM").alias("month_name"),
        F.concat(
            F.year("date_key").cast("string"),
            F.lit("-"),
            F.lpad(F.month("date_key").cast("string"), 2, "0")
        ).alias("year_month"),
        
        # Quarter attributes
        F.concat(
            F.year("date_key").cast("string"),
            F.lit("-Q"),
            F.quarter("date_key").cast("string")
        ).alias("year_quarter"),
        
        # Flags
        F.when(F.dayofweek("date_key").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        F.when(F.dayofweek("date_key").between(2, 6), True).otherwise(False).alias("is_weekday"),
        
        # Relative dates
        F.date_format("date_key", "yyyy-MM-dd").alias("date_string"),
        F.unix_timestamp("date_key").alias("date_timestamp")
    )
    
    return calendar
