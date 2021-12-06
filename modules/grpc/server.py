from concurrent import futures
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from geoalchemy2.types import Geometry as GeometryType
from marshmallow import Schema, fields
from marshmallow_sqlalchemy.convert import ModelConverter as BaseModelConverter
from dataclasses import dataclass
from datetime import datetime
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from shapely.geometry.point import Point
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy import BigInteger, Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.hybrid import hybrid_property
from secret import secret
import time
import grpc
import json
import ast

# import the generated classes
import execute_pb2
import execute_pb2_grpc

base = declarative_base()

class Persons(base):
    __tablename__ = "person"

    id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    company_name = Column(String, nullable=False)

class Location(base):
    __tablename__ = "location"

    id = Column(BigInteger, primary_key=True)
    person_id = Column(Integer, ForeignKey(Persons.id), nullable=False)
    coordinate = Column(Geometry("POINT"), nullable=False)
    creation_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    _wkt_shape: str = None

    @property
    def wkt_shape(self) -> str:
        # Persist binary form into readable text
        if not self._wkt_shape:
            point: Point = to_shape(self.coordinate)
            # normalize WKT returned by to_wkt() from shapely and ST_AsText() from DB
            self._wkt_shape = point.to_wkt().replace("POINT ", "ST_POINT")
        return self._wkt_shape

    @wkt_shape.setter
    def wkt_shape(self, v: str) -> None:
        self._wkt_shape = v

    def set_wkt_with_coords(self, lat: str, long: str) -> str:
        self._wkt_shape = f"ST_POINT({lat} {long})"
        return self._wkt_shape

    @hybrid_property
    def longitude(self) -> str:
        coord_text = self.wkt_shape
        return coord_text[coord_text.find(" ") + 1 : coord_text.find(")")]

    @hybrid_property
    def latitude(self) -> str:
        coord_text = self.wkt_shape
        return coord_text[coord_text.find("(") + 1 : coord_text.find(" ")]


class LocationSchema(Schema):
    id = fields.Integer()
    person_id = fields.Integer()
    longitude = fields.String(attribute="longitude")
    latitude = fields.String(attribute="latitude")
    creation_time = fields.DateTime()

    class Meta:
        model = Location



class InvokeServicer(execute_pb2_grpc.InvokeServicer):

    def create_person(self, request, context):
        new_person = Persons()
        new_person.first_name = request.first_name
        new_person.last_name = request.last_name
        new_person.company_name = request.company_name
        print('completed init')
        db_string = secret.get('KEY')
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        session = Session()
        qry=session.query(func.max(Persons.id).label("max_id"))
        new_person.id=(qry.one().max_id)+1
        session.add(new_person)
        session.commit()
        print(new_person)
        response = execute_pb2.Status()
        response.status = True
        return response

    def create_location(self, request, context):
        

        new_location = Location()
        new_location.person_id = request.person_id
        new_location.creation_time = datetime.now()
        new_location.coordinate = ST_Point(request.latitude, request.longitude)
        print('init')

        db_string = secret.get('KEY')
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        session = Session()
        qry=session.query(func.max(Location.id).label("max_id"))
        new_location.id=(qry.one().max_id)+1
        session.add(new_location)
        print(new_location)
        session.commit()
        response = execute_pb2.Status()
        response.status = True
        return response




server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

execute_pb2_grpc.add_InvokeServicer_to_server(
        InvokeServicer(), server)

# listen on port 50051
print('Starting server. Listening on port 5001.')
server.add_insecure_port('[::]:5001')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)