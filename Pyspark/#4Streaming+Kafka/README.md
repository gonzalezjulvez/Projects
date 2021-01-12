![Kafka Logo](https://grape.solutions/img/partners/logo_kafka.png) + ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)

# Proyecto 4: Sistema Streaming con Kafka+PrestoDb

En este proyecto esdrújula llevaremos a cabo un sistema de mensajería de datos en streaming mediante la herramienta kafka y Spark Streaming.

**Apache Kafka**

Arrancamos los servicios de kafka y zookeeper.

- zookeeper-server-start.sh config/zookeeper.properties
- kafka-server-start.sh config/server.properties

Crearemos el topic que nos permita mandar y distribuir los datos hacía los consumidores, 
en nuestro caso serán leídos por Apache Streaming.

- kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic json_topic --create --partitions 3 --replication-factor 1

Una vez creado el topic ya podemos abrir el producer para introducir los datos.

- kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic json_topic

Los mensajes por defecto kafka es capaz de almacenarlos durante una semana. Una vez creado el topic y el producer pasaremos a la creación del esquema para analizar los stream con PrestoDB

**PrestoDB**

Deberemos crear un fichero kafka.properties creándose asi una nueva fuente de datos en el siguiente texto:

connector.name=kafka 

kafka.nodes=localhost:9092

kafka.table-names:json_topic

kafka.hide-internal-columns = false


Este fichero se alojará en presto-server-0.213/etc/catalog/kafka.properties

Ya le hemos indicado a PrestoDB que datos leer, ahora deberemos indicarle cual será la estructura de esos datos.

Creamos un archivo .json en presto-server-0.213/etc/kafka/json_topic.json con la siguiente estructura.

En la nuestro caso será:

{
"tableName": "json_topic",
"topicName": "json_topic",
"schemaName": "default",
"message": {
"dataFormat":"json",
"fields": [
{
"name": "nombre",
"mapping": "nombre",
"type": "VARCHAR"
},
{
"name": "edad",
"mapping": "edad",
"type": "INT"
},
{
"name": "peso",
"mapping": "peso",
"type": "DOUBLE"
}
]
}
}


