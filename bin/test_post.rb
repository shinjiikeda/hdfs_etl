#!/usr/bin/env jruby

require 'logger'
require 'poseidon'
require 'parallel'

kafka_brokers = ["localhost:9092"]
kafka_topic = "hdfs_queue"

producer = Poseidon::Producer.new(kafka_brokers, "my_test_producer")

Parallel.each([ * 0 ... 4 ], :in_threads => 4) do |n|
  messages = []
  100000.times.each do | n |
    now = Time.now
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    hash = n % 30

    key = "db.schema.table#{hash}/#{date}/#{hour}"

    val = "#{now.to_i}\ttest.."

    messages << Poseidon::MessageToSend.new(kafka_topic, val, key)
    if messages.size > 100
      producer.send_messages(messages)
      messages = []
    end
  end
  producer.send_messages(messages) if messages.size > 0
end


