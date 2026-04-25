import sys
from insurance.ingestion.streaming.kafka_consumer import run_kafka_consumer


def main():
    base_location = sys.argv[1]

    # You can extend this later for multiple topics
    run_kafka_consumer(base_location=base_location, topic="claims")


if __name__ == "__main__":
    main()
