#!/usr/bin/env jruby

require 'logger'
require 'poseidon'
require 'optparse'

$is_daemon = false
conf_path = nil
logfile = nil

opt = OptionParser.new
opt.on('--daemon') {|v| $is_daemon = v }
opt.on('--conf path') {|v| conf_path = v }
opt.on('--logfile path') {|v| logfile = v}
opt.parse!(ARGV)

require conf_path

kafka_brokers = HdfsETLConfig::KAFKA::BROKERS
kafka_topic = HdfsETLConfig::KAFKA::TOPIC
kafka_min_brokers_num = HdfsETLConfig::KAFKA::MIN_BROKERS_NUM

producer = Poseidon::Producer.new(kafka_brokers, "my_test_producer",  :required_acks=>1, :compression_codec => :gzip)

meta =  producer.producer.cluster_metadata
meta.update(producer.producer.broker_pool.fetch_metadata([kafka_topic]))
p meta.brokers
p meta.brokers.size
#p meta.topic_metadata

hlog_queue = meta.topic_metadata[kafka_topic]

puts "topic: " +  hlog_queue.name
puts "leader available: " + hlog_queue.leader_available?.to_s
puts "part num: " + hlog_queue.partition_count.to_s
puts "part available num: " +  hlog_queue.available_partition_count.to_s

available_partitions = hlog_queue.available_partitions
available_partitions.sort! do | a, b |
  a[:id] <=> b[:id]
end

available_partitions.each do | part |
  puts "id: #{part[:id]}, leader: #{part[:leader]}, replicas: #{part[:replicas].to_s}, Isr: #{part[:isr].to_s}, error=#{part[:error]}"
end

if (meta.brokers.size < kafka_min_brokers_num)
  puts "brokers is too little"
end

