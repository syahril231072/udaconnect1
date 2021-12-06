import grpc

# import the generated classes
import execute_pb2
import execute_pb2_grpc
from execute_pb2 import Person, Location
from datetime import datetime

# open a gRPC channel
channel = grpc.insecure_channel('localhost:5001')

# create a stub (client)
stub = execute_pb2_grpc.InvokeStub(channel)

# create a valid request message
#person = Person(first_name="kalyanakannan" , last_name="Dhakshinamoorthi", company_name="Prodapt")
location = Location(person_id=10,creation_time="sdsdsd",latitude="34.518730",longitude="-117.992470")
stub.create_location(location)
print(location)

# make the call
#stub.create_person(person)

