1. FAILURE POLICY & RECOVERY MECHANISMS
We are actually pushing a List[SinkRecord]. Now if one of them fails to be inserted due to any reason, then how can the previous DB operations in the same transaction ( or the records already inserted) be rolled back? Is the same set of SinkRecords repeated again for inserting to DB? 

2. WORKER HANDLING OF PARTITIONS
We understand that the worker is used for distributing the work among any available processes & thus increases the scalability. How does it distribute? If there are any topic partitions then how does it handle? 

3. SCHEMA & DATATYPES
We use Avro schema to send the data & this avro schema is registered in Schema Registry. When we receive the same schema as KeySchema() or ValueSchema() in Kafka Sink, it is of type 
[org.apache.kafka.connect.data.Schema](http://docs.confluent.io/2.0.0/connect/javadocs/index.html?org/apache/kafka/connect/data/package-summary.html)
Now, we see that the Schema supports Int32, Int64, Float32, Float64 etc. Does the avro to kafka schema converter choose the right data types while conversion before sending to sink?
Or they choose a generic one like Float, Integer etc.?
The AvroSchema uses the logicalTypes for specifying additional info on datatypes which require parameters like decimal. It would be represented like, 

 ```
 {
   "type": "bytes",
   "logicalType": "decimal",
   "precision": 4,
   "scale": 2
 }
 ```
 
 How can the same information be received from the Kafka Schema?
 
Is the Schema plan to be improved for more robust inclusion of data types?

Is there an interface provided to improve the avro to kafka schema converter?

4. USING RESOURCE MANAGER
For managing the processes, Kafka can be integrated with any resource manager like Yarn etc. (http://docs.confluent.io/2.0.0/connect/design.html).. search for YARN here for details