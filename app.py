import json
import logging
import sys
import random
import io
import os
import asyncio
from datetime import datetime
from json import JSONDecodeError

import uvicorn

from settings import Settings
from uvicorn import Config, Server

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles

from database import MovementDoc, Entity, Big, Contragent, Object, Place, Port, DocType, Package, EntityClass, \
    TransportType, serialize_collection

from logs import get_logger

LOGGER = get_logger()

__version__ = '0.14229'

class_table = {
    'big': Big,
    'contragent': Contragent,
    'object': Object,
    'place': Place,
    'port': Port,
    'type': DocType,
    'package': Package,
    'transport': TransportType
}

app = FastAPI(title="Proton Backend")

# app.mount("/static", StaticFiles(directory='static'), name='static')

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Proton Backend",
        version=__version__,
        description="Провайдер данных для приложения склада",
        routes=app.routes,
    )
    # openapi_schema["info"]["x-logo"] = {
    #     "url": "https://api.covidget.info/static/logo.png"
    # }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/api/v1/ping")
async def ping():
    """
    Понг блять.


    :return:
    """
    return jsonable_encoder(dict(alive=True))


@app.get("/api/v1/entity/{entity_id}")
async def entity_info(entity_id):
    """
    Получить развернутую информацию по грузопозиции.

    :param entity_id:
    :return:
    """
    entity = Entity.get(entity_id)
    if not entity:
        return Response(json.dumps(dict(reason="Not Found")), status_code=404)
    else:
        return jsonable_encoder(entity.serialized)


@app.get("/api/v1/properties/{property}")
async def get_properties(property):
    """
    Метод для получения справочных элементов.

    Текущий список того, что можно получить:

    'big': Укрупненная номенклатура

    'contragent': Контрагенты

    'object': Объект

    'place': Места хранения

    'port': Порт погрузки/выгрузки

    'type': Типы документов

    'package': Упаковка

    'transport': Виды транспорта

    :param property:
    :return:
    """
    collection = class_table[property].get_all()
    data = serialize_collection(collection)
    return jsonable_encoder(data)


@app.get("/api/v1/doc")
@app.get("/api/v1/doc/{doc_id}")
@app.put("/api/v1/doc")
@app.patch("/api/v1/doc/{doc_id}")
@app.delete("/api/v1/doc/{doc_id}")
async def process_doc(request: Request, doc_id=None):
    """
    Маршрут для обработки документа движения.

    Тип зависит от указанного в теле запроса, действие с документом зависит от метода запроса.

``{
    "id": 30,
    "tag": "1",
    "contract": "1",
    "type": 1,
    "sender": 2,
    "receiver": 1,
    "transport_type": "Авто",
    "transport_tag": "1111",
    "send_date": "2021-09-01T00:00:00",
    "receive_date": "2021-09-29T00:00:00",
    "danger_class": "1",
    "port": 1,
    "object": "1-СВ3",
    "place": 1,
    "big": 1,
    "extra": "121212",
    "entities": [
        {
            "id": 7,
            "pipe_tag": null,
            "name": "1",
            "big": 1,
            "inplace_count": "1",
            "package": 2,
            "segment_number": "1",
            "weight": 1.0,
            "lenght": 2.0,
            "width": null,
            "diameter": 1.0,
            "thickness": 2.0,
            "fu": 2.0,
            "place_number": 1,
            "extra": "121212",
            "input_doc": 30,
            "output_doc": null
        }
    ]
}``

    :param request:
    :param doc_id:
    :return:
    """
    if request.method == "GET":
        LOGGER.log(logging.INFO, msg="Request doc %s" % doc_id)
        if not doc_id:
            response = []
            data = MovementDoc.get_all()
            for _ in data:
                response.append(_.serialized)
            return jsonable_encoder(response)
        else:
            data = MovementDoc.get(doc_id)
            return jsonable_encoder(data)
    elif request.method == 'PUT':
        request_body = b''
        async for chunk in request.stream():
            request_body += chunk
        try:
            message = json.loads(request_body)
        except JSONDecodeError:
            raise HTTPException(400, detail="Некорректный JSON")
        try:
            req = message
            if "entities" not in req or len(req["entities"]) == 0:
                return Response(json.dumps(dict(reason="Empty entities")), status_code=500)
            doc = MovementDoc(
                type=req['type'],
                port=req['port'],
                sender=req['sender'],
                receiver=req['receiver'],
                place=req['place'],
                transport_type=req['transport_type'],
                object=req['object'],
                danger_class=req["danger_class"],
                big=req["big"],
                transport_tag=req["transport_tag"],
                tag=req["tag"],
                send_date=datetime.strptime(req["send_date"], "%Y-%m-%d"),
                receive_date=datetime.strptime(req["receive_date"], "%Y-%m-%d"),
                extra=req["extra"],
                contract=req["contract"]
            )
            doc.save()

            to_doc = []
            for _ in req["entities"]:
                LOGGER.log(logging.INFO, msg="Process entity %s from doc %s" % (_["name"], doc.id))
                entity_class = EntityClass.get_by_name(_["name"])
                if not entity_class:
                    _entity_class = EntityClass(name=_["name"])
                    _entity_class.save()
                if 'fu' in _:
                    entity = Entity(
                        name=_['name'],
                        big=doc.big,
                        inplace_count=_['inplace_count'],
                        package=_['pipe_tag'],
                        weight=_['weight'],
                        height=_['length'],
                        segment_number=_['segment_number'],
                        diameter=_['diameter'],
                        thickness=_['thickness'],
                        place_number=_['place_number'],
                        extra=_['extra'],
                        fu=_['fu'],
                        input_doc=doc.id
                    )
                    entity.save()
                else:
                    entity = Entity(
                        name=_['name'],
                        big=doc.big,
                        inplace_count=_['inplace_count'],
                        package=_['pipe_tag'],
                        weight=_['weight'],
                        height=_['length'],
                        segment_number=_['segment_number'],
                        diameter=_['diameter'],
                        thickness=_['thickness'],
                        place_number=_['place_number'],
                        extra=_['extra'],
                        input_doc=doc.id
                    )
                    entity.save()
                LOGGER.log(logging.INFO, msg="Processed entity %s, %s" % (entity.name, entity.id))
                to_doc.append(entity.id)
            doc.entities = json.dumps(to_doc)
            doc.save()
            return jsonable_encoder(doc.serialized)
        except KeyError as e:
            doc.delete()
            return json.dumps(dict(missing_key=e.args))
        except Exception as e:
            doc.delete()
            return Response(json.dumps(dict(error=True, details=e.args)), status_code=500)
    elif request.method == 'PATCH':
        doc = MovementDoc.get(doc_id)
        if doc:
            request_body = b''
            async for chunk in request.stream():
                request_body += chunk
            try:
                message = json.loads(request_body)
            except JSONDecodeError:
                raise HTTPException(400, detail="Некорректный JSON")
            try:
                req = message
                doc.type = req['type']
                doc.port = req['port']
                doc.sender = req['sender']
                doc.receiver = req['receiver']
                doc.place = req['place']
                doc.transport_type = req['transport_type']
                doc.object = req['object']
                doc.danger_class = req["danger_class"]
                doc.big = req["big"]
                doc.transport_tag = req["transport_tag"]
                doc.tag = req["tag"]
                doc.send_date = datetime.strptime(req["send_date"], "%Y-%m-%d")
                doc.receive_date = datetime.strptime(req["receive_date"], "%Y-%m-%d")
                doc.extra = req["extra"]
                doc.contract = req["contract"]
                for _ in req['entities']:
                    entity = Entity.get(_['id'])
                    entity.name = _['name']
                    entity.big = doc.big
                    entity.inplace_count = _['inplace_count']
                    entity.package = _['pipe_tag']
                    entity.weight = _['weight']
                    entity.height = _['length']
                    if entity.segment_number != _['segment_number']:
                        entity.segment_number = _['segment_number']
                    entity.diameter = _['diameter']
                    entity.thickness = _['thickness']
                    entity.place_number = _['place_number']
                    entity.extra = _['extra']
                    entity.save(modify=True)
                doc.save(modify=True)
                return jsonable_encoder(dict(success=True))
            except KeyError as e:
                return Response(json.dumps(dict(error=True, details=e.args)), status_code=500)
            except Exception as e:
                return Response(json.dumps(dict(error=True, details=e.args)), status_code=500)
        else:
            return Response(json.dumps(dict(error=True, message="Not Found")), status_code=404)
    elif request.method == "DELETE":
        doc = MovementDoc.get(doc_id)
        if doc:
            try:
                for _ in json.loads(doc.entities):
                    entity = Entity.get(int(_))
                    entity.delete()
                doc.delete()
                return jsonable_encoder(dict(success=True))
            except KeyError as e:
                return Response(json.dumps(dict(error=True, details=e.args)), status_code=500)
            except Exception as e:
                return Response(json.dumps(dict(error=True, details=e.args)), status_code=500)
        else:
            return Response(json.dumps(dict(error=True, message="Not Found")), status_code=404)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
