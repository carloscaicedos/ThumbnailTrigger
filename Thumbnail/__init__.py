import os
import json
import logging
import numpy as np
import cv2 as cv
import azure.functions as func
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

BLOB_STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']


def getBlobNameFromUrl(blobUrl: str) -> str:
    blob_client = BlobClient.from_blob_url(blob_url=blobUrl)
    return blob_client.blob_name

def main(event: func.EventGridEvent, inputblob: func.InputStream):
    logging.info('Entró a la función Thumbnail')

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    event_data = event.get_json()
    url = event_data['url']

    if inputblob:
        img_bytes = inputblob.read()
        img_array = np.fromstring(img_bytes, dtype=np.uint8)
        img = cv.imdecode(img_array, cv.IMREAD_COLOR)

        h, w, _ = img.shape

        scale = min(300 / w, 300 / h)
        nw, nh = int(scale * w), int(scale * h)

        thumbnail = cv.resize(img, (nw, nh), cv.INTER_CUBIC)
        buffer = cv.imencode(os.path.basename(url), thumbnail)[1]

        blob_name = getBlobNameFromUrl(url)
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=BLOB_STORAGE_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client('thumbnail')

        try:
            blob_container_client.upload_blob(name=blob_name, data=buffer.tobytes(), overwrite=True)
            logging.info('Miniatura creada exitosamente.')
        except Exception as e:
            logging.error('No se pudo subir la miniatura:', str(e))

    logging.info('Python EventGrid trigger processed an event: %s', result)
