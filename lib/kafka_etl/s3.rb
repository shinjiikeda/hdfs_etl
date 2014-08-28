
require 'kafka_etl/base'
require 'aws-sdk'
require 'logger'

module KafkaETL
  class S3ETL < Base
    
    def initialize(zookeeper, kafka_brokers, kafka_topic, s3_bucket, s3_prefix, opts={})
      opt[:max_fetch_size] = opt[:max_fetch_size] ? opt[:max_fetch_size] : 100_000_000
      opt[:min_fetch_size] = opt[:min_fetch_size] ? opt[:min_fetch_size] :  10_000_000
      super(zookeeper, kafka_brokers, kafka_topic, opts)
      @s3_bucket = s3_bucket
      @s3_prefix = s3_prefix

      @s3 = AWS::S3.new(
        :access_key_id => 'YOUR_ACCESS_KEY_ID',
        :secret_access_key => 'YOUR_SECRET_ACCESS_KEY' 
      )      
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
        val = m.value.chomp
        
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
        path = sprintf("%s/%s/%s/part-%02d_%08x_%04x", @s3_prefix, prefix, date, hour, Time.now.to_i, rand(65536))
        $log.info "bucket: @s3_bucket, path: #{path}"
        bucket = s3.buckets[@s3_bucket]
        object = bucket.objects[path]
        buf = ""
        values.each do |val|
          buf <<  val
          proc_num += 1
        end
        object.write(buf) if buf.bytesize > 0
      end
      records.clear
      
      return proc_num
    end
  end
end
