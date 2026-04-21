class consumeKafkaData:
    def __init__(self, topic):
        self.topic = topic

    def read_from_kafka(self):
        """
        reading the data from the given topic from Kafka
        """
        bootstrap_server = spark.conf.get("bootstrap_server")
        kafka_key = spark.conf.get("kafka_key")
        kafka_secret = spark.conf.get("kafka_secret")
        JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        df = (
            spark.readStream.format("kafka")
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


claims = consumeKafkaData(topic="claims")
read_claims = claims.read_from_kafka()
read_claims.show()
