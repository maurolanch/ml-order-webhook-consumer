import base64
import json
import os
from datetime import datetime, timezone
from flask import Flask, request
from google.cloud import storage
import uuid

app = Flask(__name__)
storage_client = storage.Client()
BUCKET_NAME = "ml-webhook-orders-raw"

@app.route("/", methods=["POST"])
def consume_pubsub():
    envelope = request.get_json()

    if not envelope or "message" not in envelope:
        return "Bad Request: no message", 400

    pubsub_message = envelope["message"]
    data = base64.b64decode(pubsub_message["data"]).decode("utf-8")

    try:
        order_json = json.loads(data)
    except json.JSONDecodeError:
        return "Bad Request: invalid JSON", 400

    now = datetime.now(timezone.utc)
    path = (
        f"mercadolibre/webhook_orders_raw/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/order_{uuid.uuid4()}.json"
    )

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(order_json), content_type="application/json")

    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)