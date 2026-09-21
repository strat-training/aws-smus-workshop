import sys
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



BUCKET = "s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev"

GOLD_WAREHOUSE_PATH = f"{BUCKET}/03_gold/"
SILVER_WAREHOUSE_PATH = f"{BUCKET}/02_silver/"

# init spark session for iceberg
spark = SparkSession.builder \
    .appName("qbs-silver-to-gold") \
    .getOrCreate()

# Read Silver Table
logger.info(f"Reading Silver Iceberg Table: glue_catalog.{SILVER_WAREHOUSE_PATH}.fact_sales")
fact_sales = spark.read.parquet(f"{SILVER_WAREHOUSE_PATH}fact_sales/")
dim_product = spark.read.parquet(f"{SILVER_WAREHOUSE_PATH}dim_product/")

# Branch Performance
logger.info(f"Processing KPI 1/5: Branch Performance (Price vs Volume)")
branch_window = Window.partitionBy("branch_id").orderBy("month")

branch_monthly = (
    fact_sales
    .withColumn("month", f.date_trunc("month", "order_date"))
    .groupBy("branch_id", "city", "month")
    .agg(
        f.sum("total_amount").alias("revenue"),
        f.sum("quantity").alias("total_qty")
    )
    .withColumn("avg_price", f.col("revenue") / f.col("total_qty"))
    .select("branch_id", "city", "month", "revenue", "total_qty", "avg_price")
)

price_volume_decomp = (
    branch_monthly
    .withColumn("prior_revenue", f.coalesce(f.lag("revenue").over(branch_window), f.lit(0)))
    .withColumn("prior_qty", f.coalesce(f.lag("total_qty").over(branch_window), f.lit(0)))
    .withColumn("prior_price", f.coalesce(f.lag("avg_price").over(branch_window), f.lit(0)))
    .withColumn("revenue_change", f.col("revenue") - f.col("prior_revenue"))
    .withColumn("qty_change", f.col("total_qty") - f.col("prior_qty"))
    .withColumn("price_change", f.col("avg_price") - f.col("prior_price"))
    .withColumn("volume_effect", f.col("qty_change") * f.col("prior_price"))
    .withColumn("price_effect", f.col("price_change") * f.col("total_qty"))
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "city", "month", "revenue_change", "volume_effect", "price_effect", "as_of_date")
)

# Write to S3
price_volume_decomp.write.mode("overwrite").parquet(f"{GOLD_WAREHOUSE_PATH}branch_performance/")

logger.info("Successfully written Gold Parquet: branch_performance")

# Menu Quadrants
logger.info(f"Processing KPI 2/5: Menu Quadrants")
w_rev = Window.partitionBy("branch_id").orderBy(f.col("revenue"))
w_vol = Window.partitionBy("branch_id").orderBy(f.col("volume"))

product_branch = (
    fact_sales
    .groupBy("branch_id", "city", "product_id")
    .agg(
        f.sum("total_amount").alias("revenue"),
        f.sum("quantity").alias("volume")
    )
    .withColumn("revenue_tier", f.ntile(2).over(w_rev))
    .withColumn("volume_tier", f.ntile(2).over(w_vol))
    .join(dim_product, on="product_id", how="inner")
    .select("branch_id", "city", "product_id", "product_name", "revenue", "volume", "revenue_tier", "volume_tier")
)

menu_quadrant = (
    product_branch
    .withColumn(
        "quadrant",
        f.when((f.col("revenue_tier") == 2) & (f.col("volume_tier") == 2), "core")
        .when((f.col("revenue_tier") == 2) & (f.col("volume_tier") == 1), "premium/niche")
        .when((f.col("revenue_tier") == 1) & (f.col("volume_tier") == 2), "value driver")
        .otherwise("underperformer")
    )
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "city", "product_id", "product_name", "revenue", "volume", "revenue_tier", "volume_tier", "quadrant", "as_of_date")
)

# Write to S3
menu_quadrant.write.mode("overwrite").parquet(f"{GOLD_WAREHOUSE_PATH}menu_quadrant/")

logger.info("Successfully written Gold Parquet: menu_quadrant")

# Menu Rank Volatility
logger.info(f"Processing KPI 3/5: Menu Rank Volatility")

w_rank = Window.partitionBy("branch_id").orderBy(f.col("revenue").desc())

ranked_products = (
    fact_sales
    .groupBy("branch_id", "product_id")
    .agg(f.sum("total_amount").alias("revenue"))
    .withColumn("revenue_rank", f.rank().over(w_rank))
    .select("branch_id", "product_id", "revenue_rank")
)

menu_rank_volatility = (
    ranked_products
    .groupBy("product_id")
    .agg(f.stddev("revenue_rank").alias("rank_volatility"))
    .orderBy(f.col("rank_volatility").desc())
    .join(dim_product, on="product_id", how="inner")
    .withColumn("as_of_date", f.current_timestamp())
    .select("product_id", "product_name", "rank_volatility", "as_of_date")
)

# Write to S3
menu_rank_volatility.write.mode("overwrite").parquet(f"{GOLD_WAREHOUSE_PATH}menu_rank_volatility/")

logger.info("Successfully written Gold Parquet: menu_rank_volatility")


# Channel Mix and Growth Correlation
logger.info(f"Processing KPI 4/5: Channel Mix")

w_branch = Window.partitionBy("branch_id")

channel_mix = (
    fact_sales
    .groupBy("branch_id", "city", "purchase_type")
    .agg(
        f.count("*").alias("orders"),
        f.sum("total_amount").alias("revenue")
    )
    .withColumn("branch_total_revenue", f.sum("revenue").over(w_branch))
    .withColumn("pct_of_branch_revenue", f.col("revenue") / f.col("branch_total_revenue"))
    .withColumn("as_of_date", f.current_timestamp())
    .select("branch_id", "city", "purchase_type", "orders", "revenue", "pct_of_branch_revenue", "as_of_date")
)

# Write to S3
channel_mix.write.mode("overwrite").parquet(f"{GOLD_WAREHOUSE_PATH}channel_mix/")

logger.info("Successfully written Gold Parquet: channel_mix")


logger.info(f"Processing KPI 5/5: Channel Growth Correlation")

branch_online_share = (
    fact_sales
    .groupBy("branch_id")
    .agg(
        (f.sum(f.when(f.col("purchase_type") == "Online", f.col("total_amount")).otherwise(0)) / f.sum("total_amount")).alias("pct_online_revenue")
    )
)

branch_growth_rate = (
    branch_monthly
    .withColumn("prior_revenue", f.coalesce(f.lag("revenue").over(branch_window), f.lit(0)))
    .withColumn("mom_growth_rate", (f.col("revenue") - f.col("prior_revenue")) / f.col("prior_revenue"))
    .groupBy("branch_id")
    .agg(f.avg("mom_growth_rate").alias("revenue_growth_rate"))
)

channel_growth_correlation = (
    branch_online_share
    .join(branch_growth_rate, on="branch_id", how="inner")
    .select(f.corr("pct_online_revenue", "revenue_growth_rate").alias("channel_growth_correlation"))
    .withColumn("as_of_date", f.current_timestamp())
)

# Write to S3
channel_growth_correlation.write.mode("overwrite").parquet(f"{GOLD_WAREHOUSE_PATH}channel_growth_correlation/")

logger.info("Successfully written Gold Parquet: channel_growth_correlation")

logger.info("Gold Layer KPI generation completed successfully.")