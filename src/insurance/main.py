import sys
from insurance.ingestion.streaming.kafka_consumer import run_kafka_consumer
from insurance.processing.standardize_data import run_flattening_pipeline


def main():
    base_location = sys.argv[1]
    env = sys.argv[2]

    # You can extend this later for multiple topics
    run_kafka_consumer(base_location=base_location, topic="claims", env=env)

    run_flattening_pipeline(base_location=base_location, env=env, topic="claims")


if __name__ == "__main__":
    main()
