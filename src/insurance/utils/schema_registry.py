from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, DateType


def get_claims_schema() -> StructType:
    return StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("claim_amount", DoubleType(), True),
            StructField("claim_date", DateType(), True),
            StructField("claim_status", StringType(), True),
            StructField("fraud_flag", BooleanType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


def get_policy_schema():
    return (
        StructType()
        .add("policy_id", StringType())
        .add("customer_id", StringType())
        .add("premium", StringType())
        .add("status", StringType())
    )


def get_schema(topic):
    if topic == "claims":
        return get_claims_schema()
    elif topic == "policy":
        return get_policy_schema()
