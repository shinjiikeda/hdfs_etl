#!/usr/bin/env jruby

require 'kafka_etl/hdfs'
require 'optparse'


zookeeper = 'localhost:2181/hdfs_etl'
kafka_brokers = ["localhost:9092"]
kafka_topic_name = "hdfs_queue"
hdfs_prefix = "./test_etl"

kafka_client_id="hdfs_etl_consumer"
kafka_topic_part_num = 4

max_fetch_bytes = 50 * 1024 * 1024
num_threads     = 4

opt = OptionParser.new
opt.on('--zk zookeeper') {|v| zookeeper = v }
opt.on('--topic kafka_topic_name') {|v| kafka_topic_name = v}
opt.on('--kafka_brokers kafka_brokers') {|v| kafka_brokers = v}
opt.on('--hdfs_prefix hdfs_prefix_path') {|v| hdfs_prefix_path = v}
opt.on('--kafka_client_id kafka_client_id') { |v| kafka_client_id = v}
opt.parse!(ARGV)

$exit_process = false
Signal.trap(:TERM, proc{ $exit_process = true })

etl = KafkaETL::HdfsETL.new(zookeeper, kafka_brokers, kafka_topic_name, hdfs_prefix,
                         :kafka_client_id => kafka_client_id,
                         :kafka_topic_part_num => kafka_topic_part_num,
                         :max_fetch_bytes => max_fetch_bytes,
                         :num_threads => num_threads
                         )

while $exit_process == false
  etl.process
  sleep 5
end

STDERR.puts "finish"

