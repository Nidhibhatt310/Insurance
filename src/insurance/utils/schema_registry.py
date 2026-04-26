from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    IntegerType,
)


def get_agents_schema() -> StructType:
    return StructType(
        [
            StructField("agent_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("experience_years", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


def get_booking_line_schema() -> StructType:
    return StructType(
        [
            StructField("booking_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


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


def get_customers_schema() -> StructType:
    return StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


def get_policies_schema() -> StructType:
    return StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("policy_type", StringType(), True),
            StructField("premium_amount", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


def get_risk_assessment_schema() -> StructType:
    return StructType(
        [
            StructField("risk_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("risk_category", StringType(), True),
            StructField("assessment_date", DateType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
        ]
    )


def get_schema(topic):
    if topic == "agents":
        return get_agents_schema()
    elif topic == "booking_line":
        return get_booking_line_schema()
    elif topic == "claims":
        return get_claims_schema()
    elif topic == "customers":
        return get_customers_schema()
    elif topic == "policies":
        return get_policies_schema()
    elif topic == "risk_assessment":
        return get_risk_assessment_schema()
