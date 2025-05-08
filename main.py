import base64
import json
import os
import pytz
import logging
from datetime import datetime
from flask import Flask, request
from google.cloud import storage
import uuid
from google.api_core.exceptions import BadRequest, NotFound, Forbidden

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
storage_client = storage.Client()
BUCKET_NAME = "ml-webhook-orders-raw"

colombia_tz = pytz.timezone('America/Bogota')

@app.route("/", methods=["POST"])
def consume_pubsub():
    logger.debug("Iniciando procesamiento de solicitud POST.")
    
    envelope = request.get_json()
    logger.debug(f"Mensaje recibido: {envelope}")

    if not envelope or "message" not in envelope:
        logger.error("Bad Request: no message")
        return "Bad Request: no message", 400

    pubsub_message = envelope["message"]
    data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
    logger.debug(f"Mensaje base64 decodificado: {data}")

    try:
        order_json = json.loads(data)
        logger.debug(f"JSON de la orden decodificado: {order_json}")
    except json.JSONDecodeError as e:
        logger.error(f"Error al decodificar JSON: {str(e)}")
        return "Bad Request: invalid JSON", 400

    now = datetime.now(colombia_tz)
    logger.debug(f"Hora actual en Colombia: {now}")

    path = (
        f"mercadolibre/webhook_orders_raw/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/order_{uuid.uuid4()}.json"
    )
    logger.debug(f"Ruta generada para el archivo: {path}")

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(path)
        blob.upload_from_string(json.dumps(order_json), content_type="application/json")
        logger.info(f"Archivo subido exitosamente a {path}")
    except BadRequest as e:
        logger.error(f"Solicitud inválida al subir a Cloud Storage: {str(e)}")
        return "Bad Request: invalid upload parameters", 400
    except NotFound as e:
        logger.error(f"Recurso no encontrado: {str(e)}")
        return "Not Found: bucket or resource missing", 404
    except Forbidden as e:
        logger.error(f"Acceso denegado al subir archivo: {str(e)}")
        return "Forbidden: access denied", 403
    except Exception as e:
        logger.error(f"Error general al subir archivo a Cloud Storage: {str(e)}")
        return "Internal Server Error: unable to upload file", 500

    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Servidor iniciado en el puerto {port}")
    app.run(host="0.0.0.0", port=port)