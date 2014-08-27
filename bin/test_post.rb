#!/usr/bin/env jruby

require 'poseidon'

kafka_brokers = ["localhost:9092"]
kafka_topic = "hdfs_queue"

producer = Poseidon::Producer.new(kafka_brokers, "my_test_producer")


messages = []
100.times.each do | n | 
  
  key = "db.table.schema/2014-08-27/00"
  val = "test.."
  
  messages << Poseidon::MessageToSend.new(kafka_topic, val, key)
  if messages.size > 100
    producer.send_messages(messages)
    messages = []
  end
end

producer.send_messages(messages) if messages.size > 0


