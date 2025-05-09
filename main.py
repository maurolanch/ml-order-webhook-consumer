import base64
import json
import os
import pytz
import logging
from datetime import datetime
from flask import Flask, request
from google.cloud import storage
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
storage_client = storage.Client()
BUCKET_NAME = "ml-webhook-orders-raw"
colombia_tz = pytz.timezone('America/Bogota')

@app.route("/", methods=["POST"])
def consume_pubsub():
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return "Bad Request", 400

    try:
        data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        order_json = json.loads(data)
    except Exception:
        return "Invalid JSON", 400

    now = datetime.now(colombia_tz)
    path = (
        f"mercadolibre/webhook_orders_raw/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/order_{uuid.uuid4()}.json"
    )

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(path)
        blob.upload_from_string(json.dumps(order_json), content_type="application/json")
        logger.info(f"Archivo subido a {path}")
    except Exception as e:
        logger.error(f"Error al subir archivo: {e}")
        return "Error", 500

    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)