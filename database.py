from sqlalchemy import create_engine, Boolean, ForeignKey, Column, String, Float, DateTime, \
    Integer, LargeBinary, UniqueConstraint, BigInteger, ForeignKeyConstraint, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import json

import random

from settings import Settings

import logging
from logs import get_logger

LOGGER = get_logger()

config = Settings()

if config.get('database', 'engine'):
    DATABASE = {
        'drivername': config.get('database', 'engine'),
        'host': config.get('database', 'host'),
        'database': config.get('database', 'name'),
        'username': config.get('database', 'login'),
        'password': config.get('database', 'password')
    }
else:
    DATABASE = {
        'drivername': 'sqlite',
        'database': './data.db'
    }

Model = declarative_base()
dbengine = create_engine(URL(**DATABASE))
Session = sessionmaker(bind=dbengine)
session = Session()


def serialize_collection(c_list):
    data = []
    for _ in c_list:
        data.append(_.serialized)
    return data


class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]


class Contragent(Model):
    __tablename__ = 'contragent'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    @staticmethod
    def get_by_name(name):
        contragent = session.query(Contragent).filter_by(name=name).one_or_none()
        return contragent

    @staticmethod
    def get_all():
        data = session.query(Contragent).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Contract(Model):
    __tablename__ = 'contract'
    id = Column(String, primary_key=True)
    contragent = Column(Integer, ForeignKey('contragent.id'))

    @staticmethod
    def get_all():
        data = session.query(Contragent).all()
        return data


class EntityClass(Model):
    __tablename__ = 'entity_class'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, unique=True)

    @staticmethod
    def get_by_name(name):
        entity_class = session.query(EntityClass).filter_by(name=name).one_or_none()
        return entity_class

    @staticmethod
    def get_all():
        data = session.query(EntityClass).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class TransportType(Model):
    __tablename__ = 'transport_type'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, unique=True)
    extra = String(String)

    @staticmethod
    def get_by_name(name):
        transport_type = session.query(TransportType).filter_by(name=name).one_or_none()
        return transport_type

    @staticmethod
    def get_all():
        data = session.query(TransportType).all()
        return data

    @property
    def serialized(self):
        data = dict(id=self.id, type=self.name)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Transport(Model):
    __tablename__ = 'transport'
    id = Column(Integer, autoincrement=True, primary_key=True)
    tag = Column(String)
    type = Column(Integer, ForeignKey('transport_type.id'))

    @staticmethod
    def get_by_tag(tag):
        transport = session.query(Transport).filter_by(tag=tag).one_or_none()
        return transport

    @staticmethod
    def get_all():
        data = session.query(Transport).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Big(Model):
    __tablename__ = 'big'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String)

    @staticmethod
    def get_by_name(name):
        big = session.query(Big).filter_by(name=name).one_or_none()
        return big

    @staticmethod
    def get_all():
        data = session.query(Big).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Place(Model):
    __tablename__ = 'place'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String)

    @staticmethod
    def get_by_name(name):
        place = session.query(Place).filter_by(name=name).one_or_none()
        return place

    @staticmethod
    def get_all():
        data = session.query(Place).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Package(Model):
    __tablename__ = 'package'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    @staticmethod
    def get_by_name(name):
        package = session.query(Package).filter_by(name=name).one_or_none()
        return package

    @staticmethod
    def get_all():
        data = session.query(Package).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Entity(Model):
    __tablename__ = 'entity'
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    pipe_tag = Column(String, nullable=True)
    name = Column(String, ForeignKey('entity_class.name'), index=True)
    big = Column(String, ForeignKey('big.id'))
    inplace_count = Column(String, nullable=True)
    package = Column(Integer, ForeignKey('package.id'), index=True, nullable=True)
    segment_number = Column(String, unique=True, nullable=True)
    weight = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    diameter = Column(Float, nullable=True)
    thickness = Column(Float, nullable=True)
    fu = Column(Float, nullable=True)
    place_number = Column(Integer, nullable=True)
    extra = Column(String, nullable=True)
    input_doc = Column(Integer, ForeignKey('movement_doc.id'), index=True)
    output_doc = Column(Integer, ForeignKey('movement_doc.id'), index=True, nullable=True)

    def __init__(self, name, big, pipe_tag=None, inplace_count=None, package=None, segment_number=None, weight=None,
                 height=None,
                 width=None, diameter=None, thickness=None, place_number=None, extra=None, input_doc=None,
                 output_doc=None, fu=None):

        self.pipe_tag = pipe_tag
        self.name = name
        self.big = big
        self.inplace_count = inplace_count
        self.package = package
        self.segment_number = segment_number
        self.weight = weight
        self.height = height
        self.width = width
        self.diameter = diameter
        self.thickness = thickness
        self.place_number = place_number
        self.extra = extra
        self.input_doc = input_doc
        self.output_doc = output_doc
        if fu:
            self.fu = fu
        else:
            v = (float(diameter) ** 2) * float(height)
            if float(self.weight) > v:
                self.fu = self.weight
            else:
                self.fu = v

    @staticmethod
    def get_by_name(name):
        entity = session.query(Entity).filter_by(name=name).one_or_none()
        return entity

    @staticmethod
    def get(id):
        entity = session.query(Entity).filter_by(id=id).one_or_none()
        return entity

    @staticmethod
    def get_all():
        data = session.query(Entity).all()
        return data

    @property
    def serialized(self):
        return dict(
            id=self.id,
            pipe_tag=self.pipe_tag,
            name=self.name,
            big=self.big,
            inplace_count=self.inplace_count,
            package=self.package,
            segment_number=self.segment_number,
            weight=self.weight,
            height=self.height,
            width=self.width,
            diameter=self.diameter,
            thickness=self.thickness,
            fu=self.fu,
            place_number=self.place_number,
            extra=self.extra,
            input_doc=self.input_doc,
            output_doc=self.output_doc
        )

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class DocType(Model):
    __tablename__ = 'doc_type'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    processable = Column(Boolean)

    @staticmethod
    def get_by_name(name):
        doc_type = session.query(DocType).filter_by(name=name).one_or_none()
        return doc_type

    @staticmethod
    def get_all():
        data = session.query(DocType).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Port(Model):
    __tablename__ = 'port'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)

    @staticmethod
    def get_by_name(name):
        port = session.query(Port).filter_by(name=name).one_or_none()
        return port

    @staticmethod
    def get_all():
        data = session.query(Port).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class Object(Model):
    __tablename__ = 'object'
    id = Column(String, unique=True, primary_key=True)

    @staticmethod
    def get(id):
        object = session.query(Object).filter_by(id=id).one_or_none()
        return object

    @staticmethod
    def get_all():
        data = session.query(Object).all()
        return data

    @property
    def serialized(self):
        data = Serializer.serialize(self)
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class MovementDoc(Model):
    __tablename__ = 'movement_doc'
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    tag = Column(String, nullable=True)
    contract = Column(String, nullable=True, index=True)
    type = Column(Integer, ForeignKey('doc_type.id'))
    sender = Column(Integer, ForeignKey('contragent.id'))
    receiver = Column(Integer, ForeignKey('contragent.id'))
    transport_type = Column(Integer, ForeignKey('transport.type'))
    transport_tag = Column(String, ForeignKey('transport.tag'))
    send_date = Column(DateTime, nullable=True)
    receive_date = Column(DateTime, nullable=True)
    danger_class = Column(String, nullable=True)
    port = Column(Integer, ForeignKey('port.id'))
    object = Column(String, ForeignKey('object.id'))
    place = Column(Integer, ForeignKey('place.id'))
    big = Column(Integer, ForeignKey('big.id'))
    extra = Column(String, nullable=True)
    entities = Column(String)

    @staticmethod
    def get_all():
        data = session.query(MovementDoc).all()
        return data

    @staticmethod
    def get(id):
        doc = session.query(MovementDoc).filter_by(id=id)
        return doc

    @property
    def serialized(self):
        data = dict(
            id=self.id,
            tag=self.tag,
            contract=self.contract,
            type=self.type,
            sender=self.sender,
            receiver=self.receiver,
            transport_type=self.transport_type,
            transport_tag=self.transport_tag,
            send_date=self.send_date,
            receive_date=self.receive_date,
            danger_class=self.danger_class,
            port=self.port,
            object=self.object,
            place=self.place,
            big=self.big,
            extra=self.extra,
        )
        entities = json.loads(self.entities)
        entity_buf = []
        for _ in entities:
            entity = Entity.get(_)
            entity_buf.append(entity.serialized)
        data["entities"] = entity_buf
        return data

    def save(self):
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()

    def delete(self):
        try:
            session.delete(self)
            session.commit()
            del self
        except Exception as e:
            LOGGER.log(level=logging.ERROR, msg="Database error: %s " % e.args)
            LOGGER.log(level=logging.ERROR, msg="Rollback transaction.")
            session.rollback()


class People(Model):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True, autoincrement=True)

    @staticmethod
    def get_all():
        data = session.query(People).all()
        return data


Model.metadata.create_all(dbengine)
