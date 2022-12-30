import pika, requests
import os, sys, time, json
import pyqrcode, io, base64, traceback
from mailjet_rest import Client
from config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE

def sendMailjetQR(name, email, judul):
    c = pyqrcode.create(name)
    s = io.BytesIO()
    c.png(s,scale=6)
    encoded = base64.b64encode(s.getvalue()).decode("ascii")

    mailjet = Client(auth=(MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE), version='v3.1')
    data = {
      'Messages': [
                    {
                            "From": {
                                    "Email": "no-reply@terasdakwah.com",
                                    "Name": "TerasDakwah"
                            },
                            "To": [
                                    {
                                            "Email": email,
                                            "Name": name
                                    }
                            ],
                            "Subject": judul,
                            "TextPart": "Assalamualaikum %s. Bawa dan tunjukkan QR Code ini untuk registrasi kajian %s. QR Code ada pada lampiran email." % (name, judul),
                            "HTMLPart": "Assalamualaikum <b>%s</b>.<br/>Bawa dan tunjukkan QR Code ini untuk registrasi <b>%s</b><br/> <img src=\"cid:id1\" width=\"100%%\"></h3>" % (name, judul),
                            "InlinedAttachments": [
                                    {
                                            "ContentType": "image/png",
                                            "Filename": "logo.png",
                                            "ContentID": "id1",
                                            "Base64Content": encoded
                                    }
                            ]
                    }
            ]
    }
    result = mailjet.send.create(data=data)
    print(result.status_code)
    print(result.json())


def getRabbitMessage(queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        virtual_host='/',
        credentials=pika.credentials.PlainCredentials(
            username=RABBITMQ_USER,
            password=RABBITMQ_PASS
        )
    ))

    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    method_frame, header_frame, body = channel.basic_get(queue)
    if method_frame:
        try:
            rabbitdata = json.loads(body)
            if rabbitdata:
                name = rabbitdata.get("name", "Jamaah TD")
                email = rabbitdata.get("email", "")
                judul = rabbitdata.get("judul", "")

                if email!="":
                    sendMailjetQR(name, email, judul)

                channel.basic_ack(method_frame.delivery_tag)

        except Exception as e:
            print (e)
            print(traceback.format_exc())

while True:
    time.sleep(1)
    getRabbitMessage('email-qr')
