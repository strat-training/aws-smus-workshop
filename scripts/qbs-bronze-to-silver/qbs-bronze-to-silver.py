import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import *
import pyspark.sql.functions as F

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# Script generated for node S3DataSource
S3DataSource_1788340757713 = spark.read.format("csv") \
    .option("inferschema", "true") \
    .option("multiLine", "true") \
    .option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .option("sep", ",") \
    .load("s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev/01_bronze/sales_raw/")

# Script generated for node RenameColumnsTransform
RenameColumnsTransform_1788255828045 = S3DataSource_1788340757713.withColumnsRenamed({"order id" : "order_id", "date" : "order_date", "purchase type" : "purchase_type", "payment method" : "payment_method", "product" : "product_name", "price" : "unit_price", "Manager" : "manager", "City" : "city"})

# Script generated for node WithColumnsTransform
WithColumnsTransform_1788256381591 = RenameColumnsTransform_1788255828045.withColumns({"order_id": col("order_id").cast("string"), "purchase_type": col("purchase_type").cast("string"), "payment_method": col("payment_method").cast("string"), "purchase_type": col("purchase_type").cast("string"), "payment_method": col("payment_method").cast("string"), "city": col("city").cast("string"), "manager": col("manager").cast("string"), "product_name": col("product_name").cast("string"), "unit_price": col("unit_price").cast("double"), "quantity": col("quantity").cast("double"), "order_date": F.expr("""   coalesce(     to_date(order_date, 'dd-MM-yyyy'),     to_date(order_date, 'yyyy-MM-dd'),     to_date(order_date, 'dd/MM/yyyy'),     to_date(order_date, 'yyyy/MM/dd')   ) """), "manager": F.regexp_replace(F.trim(F.col("manager")), r"\s+", " "), "product_name": F.regexp_replace(F.trim(F.col("product_name")), r"\s+", " "), "purchase_type": F.regexp_replace(F.trim(F.col("purchase_type")), r"\s+", " "), "payment_method": F.regexp_replace(F.trim(F.col("payment_method")), r"\s+", " "), "city": F.regexp_replace(F.trim(F.col("city")), r"\s+", " ")})

# Script generated for node EvaluateDataQualityTransform
EvaluateDataQualityTransform_1788254718144_ruleset = """
    Rules = [
        ColumnValues \"quantity\" > 0,
        ColumnValues \"unit_price\" > 0,
        IsComplete \"unit_price\",
        IsComplete \"order_id\",
        IsComplete \"order_date\",
        ColumnValues \"product_name\" in [\"Fries\", \"Beverages\", \"Sides & Other\", \"Burgers\", \"Chicken Sandwiches\"],
        ColumnValues \"city\" in [\"London\", \"Madrid\", \"Lisbon\", \"Berlin\", \"Paris\"]
    ]
"""
EvaluateDataQualityTransform_1788254718144 = EvaluateDataQuality().process_rows(
    frame=WithColumnsTransform_1788256381591,
    ruleset=EvaluateDataQualityTransform_1788254718144_ruleset,
    publishing_options={"dataQualityEvaluationContext": "dq_etl_ruleset_bx6wr6wcsd35wh_qbs-dqdl-ruleset", "enableDataQualityCloudWatchMetrics": "false", "enableDataQualityResultsPublishing": "true", "projectId": "bx6wr6wcsd35wh"},
    additional_options={"observations.scope": "NONE"}
)

# Script generated for node SelectDataQualityOutputTransform
SelectDataQualityOutputTransform_1788327270542_rowLevelOutcomesPassed = EvaluateDataQualityTransform_1788254718144["rowLevelOutcomes"]
SelectDataQualityOutputTransform_1788327270542_rowLevelOutcomesPassed = SelectDataQualityOutputTransform_1788327270542_rowLevelOutcomesPassed.filter("`DataQualityEvaluationResult` == 'Passed'")

# Script generated for node SelectDataQualityOutputTransform
SelectDataQualityOutputTransform_1788327302397_rowLevelOutcomesFailed = EvaluateDataQualityTransform_1788254718144["rowLevelOutcomes"]
SelectDataQualityOutputTransform_1788327302397_rowLevelOutcomesFailed = SelectDataQualityOutputTransform_1788327302397_rowLevelOutcomesFailed.filter("`DataQualityEvaluationResult` == 'Failed'")

# Script generated for node DropTransform
DropTransform_1788332014151 = SelectDataQualityOutputTransform_1788327270542_rowLevelOutcomesPassed.drop("DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult")

# Script generated for node SelectTransform
SelectTransform_1788340096878 = SelectDataQualityOutputTransform_1788327302397_rowLevelOutcomesFailed.select("order_id", "order_date", "product_name", "unit_price", "quantity", "purchase_type", "payment_method", "manager", "city")

# Script generated for node WithColumnsTransform
WithColumnsTransform_1788332683370 = DropTransform_1788332014151.withColumns({"is_fractional_qty": (F.col("quantity") % 1) != 0, "branch_id": F.md5(F.col("city")), "product_id": F.md5(F.col("product_name")), "total_amount": F.round(F.col("unit_price") * F.col("quantity"), 2), "month": F.month(F.col("order_date")), "year": F.year(F.col("order_date")), "created_date": F.current_timestamp(), "last_update_date": F.current_timestamp()})

# Script generated for node S3DataSink
SelectTransform_1788340096878.write.format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .save("s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev/04_quarantine/")

# Script generated for node SelectTransform
SelectTransform_1788334838384 = WithColumnsTransform_1788332683370.select("branch_id", "city", "manager", "created_date", "last_update_date")

# Script generated for node SelectTransform
SelectTransform_1788334991770 = WithColumnsTransform_1788332683370.select("order_id", "branch_id", "product_id", "order_date", "purchase_type", "payment_method", "unit_price", "quantity", "total_amount", "is_fractional_qty", "city", "month", "year", "created_date", "last_update_date")

# Script generated for node SelectTransform
SelectTransform_1788335103394 = WithColumnsTransform_1788332683370.select("product_id", "product_name", "created_date", "last_update_date")

# Script generated for node DropDuplicatesTransform
DropDuplicatesTransform_1788408668894 = SelectTransform_1788334838384.dropDuplicates(["branch_id", "city"])

# Script generated for node DropDuplicatesTransform
DropDuplicatesTransform_1788408735664 = SelectTransform_1788334991770.dropDuplicates(["order_id", "product_id", "branch_id"])

# Script generated for node DropDuplicatesTransform
DropDuplicatesTransform_1788408818778 = SelectTransform_1788335103394.dropDuplicates(["product_id"])

# Script generated for node S3DataSink
DropDuplicatesTransform_1788408668894.write.format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .save("s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev/02_silver/dim_branch/")

# Script generated for node S3DataSink
DropDuplicatesTransform_1788408735664.write.format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .save("s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev/02_silver/fact_sales/")

# Script generated for node S3DataSink
DropDuplicatesTransform_1788408818778.write.format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .save("s3://amazon-sagemaker-703306584987-ap-southeast-1-qbs-smus/dzd-beaiy3asjwltcx/bx6wr6wcsd35wh/dev/02_silver/dim_product/")
