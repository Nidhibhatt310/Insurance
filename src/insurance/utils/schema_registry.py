from pyspark.sql.types import StructType, StringType


def get_claims_schema():
    return (
        StructType()
        .add("claim_id", StringType())
        .add("policy_id", StringType())
        .add("amount", StringType())
        .add("status", StringType())
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
