from __future__ import annotations
from kafka import KafkaConsumer
from dataclasses import dataclass
from datetime import datetime
from execute_pb2 import Person, Location
import execute_pb2
import execute_pb2_grpc
import json
import ast


def create_person(req):
    channel = grpc.insecure_channel('host.docker.internal:5001')
    stub = execute_pb2_grpc.InvokeStub(channel)
    person = Person(first_name=req["first_name"] , last_name=req["last_name"], company_name=req["company_name"])
    stub.create_person(person)


def create_location(req):
    channel = grpc.insecure_channel('host.docker.internal:5001')
    stub = execute_pb2_grpc.InvokeStub(channel)
    location = Location(person_id=req["person_id"],latitude=req["person_id"],longitude=req["longitude"])
    stub.create_location(location)


consumer = KafkaConsumer('sample',
     bootstrap_servers=['kafka:30092'],
     value_deserializer=lambda m: json.dumps(m.decode('utf-8')))

for message in consumer:
    resp=eval(json.loads((message.value)))
    if "first_name" in resp:
        create_person(resp)
    else:
         create_location(resp)
