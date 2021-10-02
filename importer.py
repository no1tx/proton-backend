import pandas as pd
import json
from database import Contragent, MovementDoc, Entity, EntityClass, DocType, Transport, Big, Package, Port, Object, TransportType

df = pd.read_excel('otchet.xlsx', sheet_name='Лист1')

for _ in range(1, df.size):
    doc_num = df.values[_][0]
    provider = df.values[_][1]
    entity_class = df.values[_][3]
    entity_big = df.values[_][4]
    entity_serial = df.values[_][5]
    package = df.values[_][7]
    weight = df.values[_][13]
    transport = df.values[_][22]
    transport_type = df.values[_][21]
    port = df.values[_][18]
    object = df.values[_][2]
    print(doc_num, provider, entity_class, entity_big, entity_serial, package, weight, transport)

    contragent = Contragent.get_by_name(provider)
    if not contragent:
        new = Contragent(name=provider)
        new.save()

    big = Big.get_by_name(entity_big)
    if not big:
        new = Big(name=entity_big)
        new.save()

    _package = Package.get_by_name(package)
    if not _package:
        new = Package(name=package)
        new.save()

    _transport_type = TransportType.get_by_name(transport_type)
    if not _transport_type:
        new = TransportType(name=_transport_type)
        new.save()

    _transport = Transport.get_by_tag(transport)
    if not _transport:
        new = Transport(tag=transport, type=transport_type)
        new.save()

    for _dt in ("Приёмка", "Отгрузка", "Внутреннее перемещение"):
        dt = DocType.get_by_name(_dt)
        if not dt:
            new_dt = DocType(name=_dt)
            new_dt.save()

    _port = Port.get_by_name(port)
    if not _port:
        new_port = Port(name=port)
        new_port.save()

    if type(object) == str and len(object) > 0:
        _object = Object.get(object)
        if not _object:
            new_object = Object(id=object)
            new_object.save()
