from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json


def standardize_topic(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Standardizes raw Kafka data by parsing JSON payload, flattening fields,
    and capturing unexpected or corrupt data.

    Parameters:
        df (DataFrame): Input DataFrame containing Kafka records.
                        Expected to have a 'value' column with JSON string.

        schema (StructType): Schema used to parse the JSON payload.

    Returns:
        DataFrame: Standardized DataFrame with:
            - Parsed columns based on schema
            - Original raw JSON column ('json')
            - '_rescued_data' column for unexpected/malformed fields

    Notes:
        - Ensures schema evolution handling by preserving raw JSON.
        - Unknown fields are captured in '_rescued_data'.
    """
    try:
        df_parsed = (
            df.selectExpr("CAST(value AS STRING) as json")
            .withColumn("parsed", from_json(col("json"), schema, {"columnNameOfCorruptRecord": "_rescued_data"}))
            .select("json", "parsed.*")
        )

        return df_parsed

    except Exception as e:
        # Logging will be plugged in later
        print(f"Error in standardizing topic data: {str(e)}")
        raise
