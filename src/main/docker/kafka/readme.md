/**
* TO CREATE THE TOPICS inside the kafka docker container:
* docker exec -it broker bash 
* kafka-topics --bootstrap-server localhost:9092 --topic orders-by-user --create
* kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create
* kafka-topics --bootstrap-server localhost:9092 --topic discounts --create
* kafka-topics --bootstrap-server localhost:9092 --topic orders --create
* kafka-topics --bootstrap-server localhost:9092 --topic payments --create
* kafka-topics --bootstrap-server localhost:9092 --topic paid-orders --create
  */