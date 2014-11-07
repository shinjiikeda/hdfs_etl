
require 'kafka_etl_base/base'
require 'hdfs_jruby'
require 'hdfs_jruby/file'
require 'logger'

module HdfsETL
  class BackendError < KafkaETLBase::BackendError
  end

  class ETL < KafkaETLBase::Base
    
    def initialize(zookeeper, kafka_brokers, kafka_topic, hdfs_prefix, opts={})
      super(zookeeper, kafka_brokers, kafka_topic, opts)
      @hdfs_prefix = hdfs_prefix
      
      if ! Hdfs.exists?(@hdfs_prefix)
        Hdfs.mkdir(@hdfs_prefix)
      end
    end
    
    def process()
      super()
      sleep(10)
    end
    
    def process_messages(cons, part_no)
      proc_num = 0
      
      records = {}
      messages = cons.fetch
      messages.each do |m|
        proc_num += 1
        key = m.key
        val = m.value
        val.force_encoding('ascii-8bit')
        val << "\n" if ! val.end_with?("\n")
        
        if key.nil? || key.size > 512
          key = "trash/#{Time.now.strftime('%Y-%m-%d/%H')}"
        end
        if records.has_key?(key)
          records[key] <<  val
        else
          records[key] = val
        end
      end
      
      records.each do | key, values |
        next if values.bytesize == 0
        (prefix, date, hour) =  key.split("/")
        (preix, hash) = prefix.split(":", 2)
        
        prefix.gsub!(/\./, "/")
        path = sprintf("%s/%s/%s/part-%05d_%02d", @hdfs_prefix, prefix, date, hour, hash)
        
        if Hdfs.exists?(path)
          filesize = Hdfs.size(path)
          mode = "a"
        else
          filesize = 0
          mode = "w"
        end
        
        begin
          $log.info "part: #{part_no}, path: #{path} size: #{filesize} start"
          Hdfs::File.open(path, mode) do | io |
            io.write values
          end
          current_filesize = Hdfs.size(path)
          $log.info("part: #{part_no}, path: #{path} size: #{current_filesize} success")
        rescue IOError, java.io.IOException => e
          if e.class == Java::OrgApacheHadoopIpc::RemoteException && e.to_s =~ /^org\.apache\.hadoop\.hdfs\.protocol\.AlreadyBeingCreatedException/
            $log.error("path: #{path} failure! broken file")
            _dir =  File.dirname(path)
            broken_dir = sprintf("%s/lost+found/%s", @hdfs_prefix, _dir)
            if ! Hdfs.exists?(broken_dir)
              Hdfs.mkdir(broken_dir)
            end
            $log.info "move #{path} => #{broken_dir}"
            Hdfs.move(path, broken_dir)
            raise BackendError, e.to_s
          else
            $log.error "class: #{e.class}, message: #{e.to_s}"
            $log.error e.backtrace
            current_filesize = Hdfs.size(path)
            $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
            $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
          end
          
          raise BackendError, e.to_s
        rescue => e
          # unkown error
          $log.error "class: #{e.class}, message: #{e.to_s}"
          $log.error e.backtrace
          $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
          $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
          raise BackendError, e.to_s
        end
      end
      
      return proc_num
    end
  end
end

