
require 'kafka_etl/base'
require 'hdfs_jruby'
require 'hdfs_jruby/file'
require 'logger'

module KafkaETL
  class HdfsETL < Base
    
    def initialize(zookeeper, kafka_brokers, kafka_topic, hdfs_prefix, opts={})
      super(zookeeper, kafka_brokers, kafka_topic, opts)
      @hdfs_prefix = hdfs_prefix
      
      if ! Hdfs.exists?(@hdfs_prefix)
        Hdfs.mkdir(@hdfs_prefix)
       end
    end
    
    def process()
      super()
      sleep(5)
    end
    
    def process_messages(cons)
      proc_num = 0
      
      records = {}
      messages = cons.fetch
      messages.each do |m|
        key = m.key
        val = m.value
        val.force_encoding('ascii-8bit')
        
        if key.nil? || key.size > 512
          key = "trash/#{Time.now.strftime('%Y-%m-%d/%H')}"
        end
        if records.has_key?(key)
          records[key] <<  val
        else
          records[key] = [ val ]
        end
      end
      
      records.each do | key, values |
        (prefix, date, hour) =  key.split("/")
        (preix, hash) = prefix.split(":", 2)
        
        prefix.gsub!(/\./, "/")
        path = sprintf("%s/%s/%s/part-%02d_%02d", @hdfs_prefix, prefix, date, hour, hash)
        $log.info "path: #{path}"
        begin
          Hdfs::File.open(path, "a") do | io |
            values.each do |val|
              val << "\n" if ! val.end_with?("\n")
              io.print val
              proc_num += 1
            end
          end
        rescue Java::JavaIo::IOException => e
          if e.class == Java::OrgApacheHadoopIpc::RemoteException && e.to_s =~ /^org\.apache\.hadoop\.hdfs\.protocol\.AlreadyBeingCreatedException/
            $log.error("path: #{path} failure! broken file")
            broken_dir = sprintf("%s/lost+found/%s/%s/", @hdfs_prefix, prefix, date)
            if ! Hdfs.exists?(broken_dir)
              Hdfs.mkdir(broken_dir)
            end
            $log.info "move #{path} => #{broken_dir}"
            Hdfs.move(path, broken_dir)
            raise BackendError, e.to_s
          else
            $log.error "class: #{e.class}, message: #{e.to_s}"
            $log.debug e.backtrace
            current_filesize = Hdfs.size(path)
            $log.error("path: #{path} size: #{current_filesize} failure!")
            $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
          end
          
          raise BackendError, e.to_s
        rescue => e
          # unkown error
          $log.error "class: #{e.class}, message: #{e.to_s}"
          $log.debug e.backtrace
          $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
          $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
          raise BackendError, e.to_s
        end
      end
      records.clear
      
      return proc_num
    end
  end
end
