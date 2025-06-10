import io
import json
import avro.schema
from avro.io import DatumReader, BinaryDecoder
from kafka import KafkaConsumer

class AvroKafkaConsumer:
    def __init__(self, config_path="config.json"):
        self.config = self.load_config(config_path)
        self.schema = self.load_schema(self.config["schema_path"])
        self.consumer = KafkaConsumer(
            self.config["kafka_topic"],
            bootstrap_servers=self.config["kafka_bootstrap_servers"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: m 
        )

    def load_config(self, path):
        with open(path) as f:
            return json.load(f)

    def load_schema(self, schema_path):
        with open(schema_path) as f:
            return avro.schema.parse(f.read())

    def avro_decode(self, msg_bytes):
        bytes_reader = io.BytesIO(msg_bytes)
        decoder = BinaryDecoder(bytes_reader)
        reader = DatumReader(self.schema)
        return reader.read(decoder)

    def consume(self):
        for message in self.consumer:
            try:
                decoded = self.avro_decode(message.value)
                print("Decoded message:", json.dumps(decoded, indent=2))
            except Exception as e:
                print("Error decoding:", e)

if __name__ == "__main__":
    consumer = AvroKafkaConsumer()
    consumer.consume()