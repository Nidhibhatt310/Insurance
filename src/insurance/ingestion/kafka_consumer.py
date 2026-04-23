import sys
base_location = sys.argv[1]

class consumeKafkaData:
<<<<<<< HEAD
    
=======
>>>>>>> 8a41b1d7a0a0deb5a8ea424a47919dc4dbeb7c0e
    def __init__(self, topic):
        self.topic = topic

    def read_from_kafka(self):
        """
        reading the data from the given topic from Kafka
        """
        bootstrap_server = spark.conf.get("bootstrap_server")
        kafka_key = spark.conf.get("kafka_key")
        kafka_secret = spark.conf.get("kafka_secret")
        JAAS_MODULE = 'org.apache.kafka.common.security.plain.PlainLoginModule'
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_server)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config", f'{JAAS_MODULE} required username="{kafka_key}" password="{kafka_secret}";'
            )
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        return df

    def write_to_bronze(self, df):
        return (
            df.writeStream
            .format('delta')
            .option('checkpointLocation',f'{base_location}/bronze/{self.topic}')
            .queryName("kafka_bronze_stream")
            .outputMode('append')
            .trigger(availableNow=True)
            .toTable(f'insurance_dev.bronze.{self.topic}')
        )



claims = consumeKafkaData(topic="claims")
<<<<<<< HEAD
read_claims_df = claims.read_from_kafka()

claims.write_to_bronze(read_claims_df)
=======
read_claims = claims.read_from_kafka()
read_claims.show()
>>>>>>> 8a41b1d7a0a0deb5a8ea424a47919dc4dbeb7c0e
