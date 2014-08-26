
require 'kafka_etl/base'
require 'hdfs_jruby'
require 'hdfs_jruby/file'
require 'logger'

module KafkaETL
  class Hdfs
    
    def initialize(zookeeper, kafka_brokers, kafka_topic, hdfs_prefix, opts={})
      super(zookeeper, kafka_brokers, kafka_topic, opts)
      @hdfs_prefix = hdfs_prefix
      
      if ! Hdfs.exists?(@hdfs_prefix)
        Hdfs.mkdir(@hdfs_prefix)
       end
    end
    
    def process()
      super()
    end
    
    def process_messages(cons)
      proc_num = 0
      
      records = {}
      messages = cons.fetch
      messages.each do |m|
        key = m.key
        val = m.value.chomp!
        
        if records.has_key?(key)
          records[key] <<  val
        else
          records[key] = [ val ]
        end
      end
      
      records.each do | key, values |
        if key.nil?
          key = "trash/#{Time.now.strftime('%Y-%m-%d/%H')}"
        end
        (key, hash) = key.split(":", 2)
        (prefix, date, hour) =  key.split("/")
        prefix.gsub!(/\./, "/")
        path = sprintf("%s/%s/%s/part-%02d_%02d", HDFS_PREFIX, prefix, date, hour, hash)
        STDERR.puts "path: #{path}"
        Hdfs::File.open(path, "a") do | io |
          values.each do |val|
            io.puts val
            proc_num += 1
          end
        end
      end
      records.clear
      
      return proc_num
    end
  end
end
