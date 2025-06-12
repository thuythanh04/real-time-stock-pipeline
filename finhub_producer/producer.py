import json
import avro.schema
import io
from avro.io import DatumWriter, BinaryEncoder
from kafka import KafkaProducer
import websocket

class FinnhubKafkaProducer:
    """
    A Kafka producer that connects to Finnhub's WebSocket API,
    receives real-time stock data, encodes it using Avro, and sends it to a Kafka topic.
    """
    def __init__(self, config_path="config.json"):
        """Initialize the FinnhubKafkaProducer with configurations"""
        self.config = self.load_config(config_path)
        self.schema = self.load_schema(self.config["schema_path"])
        self.producer = KafkaProducer(bootstrap_servers=self.config["kafka_bootstrap_servers"])

    def load_config(self, path):
        """Load configuration settings from a JSON file"""
        with open(path) as f:
            return json.load(f)

    def load_schema(self, path):
        """Load and parse the Avro schema from a file"""
        with open(path) as f:
            return avro.schema.parse(f.read())

    def avro_encode(self, message_dict):
        """
        Encode a Python dictionary into Avro binary format.

        Args:
            message_dict (dict): Data to encode.

        Returns:
            bytes: Avro-encoded binary data.
        """
        buffer = io.BytesIO()
        encoder = BinaryEncoder(buffer)
        writer = DatumWriter(self.schema)
        writer.write(message_dict, encoder)
        return buffer.getvalue()

    def on_message(self, ws, message):
        """
        Callback triggered when a WebSocket message is received.
        Parses the JSON message, encodes it in Avro format, and sends it to Kafka.

        Args:
            message (str): The raw message received from the WebSocket.
        """
        print(">> Raw WebSocket message:", message)
        try:
            data = json.loads(message)
            if "data" not in data:
                print(">> No trade data, skipping.")
                return
            
            # Wrap and encode the message
            avro_message = self.avro_encode({
                'data': data.get('data'),
                'type': data.get('type')
            })
            
            # Send to Kafka topic
            self.producer.send(self.config["kafka_topic"], avro_message)
            print(f">> Published to Kafka: {data}")

        except Exception as e:
            print("Error:", e)

    def on_error(self, ws, error):
        """ Callback when the WebSocket encounters an error """
        print("WebSocket error:", error)

    def on_close(self, ws, *args):
        """ Callback when the WebSocket connection is closed """
        print("WebSocket closed")

    def on_open(self, ws):
        """
        Callback when the WebSocket connection is opened.
        - Subscribes to multiple stock symbols.
        """
        for sym in self.config["symbols"]:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

    def run(self):
        """
        Starts the WebSocket client and listens to Finnhub live data.
        """
        websocket.enableTrace(False)
        ws_url = f"wss://ws.finnhub.io?token={self.config['finnhub_token']}"
        ws = websocket.WebSocketApp(
            ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.on_open = self.on_open
        ws.run_forever()

if __name__ == "__main__":
    producer = FinnhubKafkaProducer()
    producer.run()