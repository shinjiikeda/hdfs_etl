#!/usr/bin/env ruby
# coding: ascii-8bit

require 'logger'
require 'poseidon'
require 'parallel'
require 'thread'
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

$kafka_brokers = HdfsETLConfig::KAFKA::BROKERS
$kafka_topic = HdfsETLConfig::KAFKA::TOPIC
$kafka_min_brokers_num = HdfsETLConfig::KAFKA::MIN_BROKERS_NUM

QUEUE_PATH=HdfsETLConfig::POST_KAFKA::QUEUE_PATH

$exit_process = false
Signal.trap(:TERM, proc{ $exit_process = true })

STDOUT.sync=true
if logfile.nil?
  $logger = Logger.new(STDOUT)
else
  $logger = Logger.new(logfile, shift_age = 10, shift_size = 10485760)
end
$logger.level = Logger::INFO

def main
  if $is_daemon == false
    post_messages
  else
    while $exit_process == false
      post_messages
      sleep 10
    end
  end
  $logger.info("finish")
end

def post_messages
  # check kafka cluster
  if ! check_kafka_cluster()
    raise "kafka is abnormality."
  end
  
  hdfs_files = []
  Dir.glob("#{QUEUE_PATH}/*").each do |f|
    next if f.end_with?('.tmp') 
    hdfs_files << f
    break if hdfs_files.size >= 50
  end
  
  return if hdfs_files.size == 0
  
  $logger.info("start")
  
  Parallel.each(hdfs_files, :in_threads => 8) do |file|
    err_queue = RetryQueue.new()
    producer = nil
    begin
      producer = Poseidon::Producer.new($kafka_brokers,
                                        "hdfs_producer",
                                        :required_acks=>1,
                                        :compression_codec => :gzip
                                        )
      
      messages = []
      message_bytes = 0
      
      rec_cnt = 0
      send_num = 0
      skip_num = 0
      pend_num = 0
      
      tmpfile = file + '.' + $$.to_s + '.' + Time.now.to_i.to_s + '.tmp'
      File.rename(file, tmpfile)
      
      File.open(tmpfile, "r") do | io |
        io.flock(File::LOCK_EX)
        while line = io.gets
          rec_cnt+=1
          line.force_encoding('ascii-8bit')
          if line.bytesize == 0 || line.bytesize > 2_000_000 || line !~ /^[A-Za-z0-9\.\-\_\:]+\t\d+\t./
            skip_num += 1
            next
          end
          
          hdfs_path, time, message = line.split("\t", 3)
          if hdfs_path.length > 1000
            skip_num += 1
            next
          end
          hdfs_path, hash = hdfs_path.split(":")
          
          t = Time.at(time.to_i)
          
          date = t.strftime("%Y-%m-%d")
          hour = t.strftime("%H")
          hash = 0
          
          key = "#{hdfs_path}:#{hash}/#{date}/#{hour}"
          val = message
          #logger.debug("key: #{key}")
          #logger.debug("val: #{val}")
          
          messages = Poseidon::MessageToSend.new($kafka_topic, val, key)
          message_bytes += val.bytesize
          
          if messages.size > 500 || message_bytes > 5_000_000
            err_num = send_messages(producer, messages, err_queue)
            send_num += (messages.size - err_num)
            pend_num += err_num
            messages = []
            message_bytes = 0
          end
        end
      end
      
      if messages.size > 0
        err_num = send_messages(producer, messages, err_queue)
        send_num += (messages.size - err_num)
        pend_num += err_num
      end
      File.delete(tmpfile)
      
      $logger.info("#{file} rec_cnt: #{rec_cnt}, send_num: #{send_num}, skip: #{skip_num} pending: #{pend_num}")
    rescue java.lang.OutOfMemoryError => e
      raise e
    rescue => e
      $logger.error(e.to_s)
      $logger.error(e.backtrace.to_s)
    ensure
      err_queue.flush()
      producer.close if ! producer.nil?
    end
    
  end
  $logger.info("end")
end

def send_messages(producer, messages, queue)
  begin
    fail_messages = producer.send_messages2(messages) if ! messages.nil?
    if ! fail_messages || fail_messages.size == 0
      return 0
    end
    $logger.error("send_messages failed")
    store_failed_messsages(fail_messages, queue)
    return fail_messages.size
  rescue => e
    $logger.error("send_messages failed")
    $logger.error(e.to_s)
    store_failed_messsages(messages, queue)
    return messages.size
  end
end

def store_failed_messsages(messages, queue)
  messages.each do | m |
    queue.push(m.value)
  end
end

def check_kafka_cluster()
  #  check
  producer = nil
  begin
    producer = Poseidon::Producer.new($kafka_brokers, "hdfs_producer")
     
    broker_pool = producer.producer.broker_pool
    meta = producer.producer.cluster_metadata
    meta.update(broker_pool.fetch_metadata([$kafka_topic]))
    broker_pool.update_known_brokers(meta.brokers)

    if (meta.brokers.size < $kafka_min_brokers_num)
      $logger.error("brokers is too little")
      return false
    end
    true
  rescue => e
    $logger.error(e.to_s)
    $logger.error(e.backtrace)
    false
  ensure
    producer.close if ! producer.nil?
  end
end

class RetryQueue
  def initialize()
    @q = []
  end
  
  def add(val)
    @q << val
    flush() if @q.size > 1000
  end
 
  def push(val)
    @q << val
    flush() if @q.size > 1000
  end
 
  def flush()
    return if @q.empty?
    
    th_id = Thread.current.object_id
    outfile = "#{QUEUE_PATH}/retry_hdfs.#{$$}.#{th_id}." + Time.now.to_i.to_s
    
    File.open(outfile, 'a') do | io |
      io.flock(File::LOCK_EX)
      @q.each do | v |
        io.puts(v)
      end
    end
    @q.clear
    $logger.info("store retry messages to #{outfile}")
   end
   
   def size
     @q.size
   end
end

main

