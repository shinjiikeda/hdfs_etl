#!/usr/bin/env jruby

require 'hdfs_etl'
require 'optparse'
require 'logger'

is_daemon = false

conf_path = nil
logfile = nil

opt = OptionParser.new
opt.on('--daemon') {|v| is_daemon = v }
opt.on('--logfile path') {|v| logfile = v}
opt.on('--conf path') {|v| conf_path = v}
opt.parse!(ARGV)

require conf_path

kafka_brokers = HdfsETLConfig::KAFKA::BROKERS
kafka_topic_name = HdfsETLConfig::KAFKA::TOPIC
kafka_topic_part_num = HdfsETLConfig::KAFKA::TOPIC_PART_NUM

zookeeper = HdfsETLConfig::ETL::ZOOKEEPER
hdfs_user = HdfsETLConfig::ETL::HDFS_USER
hdfs_prefix = HdfsETLConfig::ETL::HDFS_PREFIX
kafka_client_id = HdfsETLConfig::ETL::KAFKA_CLIENT_ID
num_threads = HdfsETLConfig::ETL::NUM_THREADS
max_fetch_bytes = MedjedHlogETLConfig::ETL::MAX_FETCH_BYTES

if logfile.nil?
  $log = Logger.new(STDOUT)
else
  $log = Logger.new(logfile, shift_age = 10, shift_size = 10485760)
end
$log.level = Logger::INFO

$exit_process = false
Signal.trap(:TERM, proc{ $exit_process = true })

etl = HdfsETL::ETL.new(zookeeper, kafka_brokers, kafka_topic_name, hdfs_prefix,
                         :kafka_client_id => kafka_client_id,
                         :kafka_topic_part_num => kafka_topic_part_num,
                         :max_fetch_bytes => max_fetch_bytes,
                         :hdfs_user => hdfs_user,
                         :num_threads => num_threads
                         )

if ! is_daemon
  etl.process
else
  while $exit_process == false
    etl.process
    sleep 15
  end
end

STDERR.puts "finish"

