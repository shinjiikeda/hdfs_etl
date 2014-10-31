#!/usr/bin/env ruby
# coding: ascii-8bit

require 'logger'
require 'poseidon'
require 'parallel'

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

$kafka_brokers = MedjedHlogETLConfig::KAFKA::BROKERS
$kafka_topic = MedjedHlogETLConfig::KAFKA::TOPIC

QUEUE_PATH=MedjedHlogETLConfig::POST_KAFKA::QUEUE_PATH

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
    post_hlog
  else
    while $exit_process == false
      post_hlog
      sleep 10
      $logger.info("finish")
    end
  end
end

def post_hlog
  $logger.info("start")
  hlog_files = []
  Dir.glob("#{QUEUE_PATH}/*").each do |f|
    next if f.end_with?('.tmp') || f.end_with?('.end')
    hlog_files << f
  end
 
  Parallel.each(hlog_files, :in_threads => 4) do |file|
    begin
      producer = Poseidon::Producer.new($kafka_brokers,
                                        "hlog_producer",
                                        :required_acks=>1,
                                        :compression_codec => :gzip
                                        )
      
      messages = []
      message_bytes = 0
      
      rec_cnt = 0
      send_num = 0
      skip_num = 0
      
      tmpfile = file + '.' + $PROCESS_ID.to_s + '.' + Time.now.to_i.to_s + '.tmp'
      File.rename(file, tmpfile)
      
      File.open(tmpfile, "r") do | io |
        while line = io.gets
          rec_cnt+=1
          #line.chomp!
          line.force_encoding('ascii-8bit')
          if line.bytesize == 0 || line.bytesize > 800_000 || line !~ /^[A-Za-z0-9\.\-\_]+\t\d+\t./
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
        
          key = "#{hdfs_path}:#{hash}/#{date}/#{hour}"
          val = message
          #logger.debug("key: #{key}")
          #logger.debug("val: #{val}")
          
          messages << Poseidon::MessageToSend.new($kafka_topic, val, key)
          message_bytes += val.bytesize
          
          if messages.size > 1000 || message_bytes > 5_000_000
            begin
              producer.send_messages(messages)
              send_num += messages.size
            rescue
              $logger.error("skip ..")
            end
            messages = []
            message_bytes = 0
          end
        end
      end
      
      if messages.size > 0
        begin
          producer.send_messages(messages) 
          send_num += messages.size
        rescue
          $logger.error("skip ..")
        end
      end
      File.delete(tmpfile)
      
      $logger.info("#{file} rec_cnt: #{rec_cnt}, send_num: #{send_num}, skip: #{skip_num}")
  rescue => e
      $logger.error(e.to_s)
      $logger.error(e.backtrace.to_s)
    end
  
  end
  $logger.info("end")
end

main

