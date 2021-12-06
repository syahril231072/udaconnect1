from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
msg1=dict({'first_name':'kalyanakannan','last_name':'dhakshinamoorthi','company_name':'prodapt'})
msg2=dict({'person_id':2 , 'creation_time':'2020-08-15 10:37:06.000000', 'latitude' : '13.082680','longitude':'80.270721'})
print(type(msg))
producer.send('sample', bytes(str(msg1), 'utf-8'))
producer.send('sample', bytes(str(msg2), 'utf-8'))
producer.flush()