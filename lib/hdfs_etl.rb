
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
      @hdfs_user = opts[:hdfs_user] ? opts[:hdfs_user] : nil
      
      if ! @hdfs_user.nil?
        Hdfs.connectAsUser(@hdfs_user)
      end
    end
    
    def process()
      
      if ! Hdfs.exists?(@hdfs_prefix)
        Hdfs.mkdir(@hdfs_prefix)
      end
      
      if ! test_hdfs()
        $log.error("hdfs is readonly or down...")
        return
      end
      
      super()
      sleep(10)
    end
    
    def process_messages(cons, part_no)
      proc_num = 0
      
      records = {}
      cons.fetch.each do |m|
        proc_num += 1
        key = m.key
        val = m.value
        next if val.nil?
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
        path = sprintf("%s/%s/%s/part-%05d_%02d", @hdfs_prefix, prefix, date, hour.to_i, hash.to_i)
        
        if ! Hdfs.exists?(File.dirname(path))
          Hdfs.mkdir(File.dirname(path))
        end
        
        if Hdfs.exists?(path)
          filesize = Hdfs.size(path)
          mode = "a"
        else
          filesize = 0
          mode = "w"
        end
        
        begin
          $log.info "part: #{part_no}, path: #{path} size: #{filesize} start"
          is_success = false
          3.times.each do | n |
            begin
              $log.info("retry..") if n > 0
              Hdfs::File.open(path, mode) do | io |
                io.write values
              end
              current_filesize = Hdfs.size(path)
              $log.info("part: #{part_no}, path: #{path} size: #{current_filesize} success")
              is_success = true
              break
            rescue org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException => e
              $log.error("path: #{path} failure! broken file")
              _dir =  File.dirname(path)
              broken_dir = sprintf("%s/lost+found/%s", @hdfs_prefix, _dir)
              if ! Hdfs.exists?(broken_dir)
                Hdfs.mkdir(broken_dir)
              end
              $log.info "move #{path} => #{broken_dir}"
              Hdfs.move(path, broken_dir)
            rescue IOError, java.io.IOException => e
              $log.error e.inspect
              $log.error e.backtrace
              current_filesize = Hdfs.size(path)
              $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
              $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
            end
            sleep 10
          end
          if ! is_success
            current_filesize = 0 if current_filesize.nil?
            $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
            raise BackendError, "part: #{part_no}, path: #{path} failure!"
          end
        rescue => e
          # unkown error
          $log.error e.inspect
          $log.error e.backtrace
          current_filesize = 0 if current_filesize.nil?
          $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
          $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
          raise BackendError, e.to_s
        end
      end
      
      return proc_num
    end
  
    def test_hdfs()
      hostname = `hostname`.chomp!
      tmpfile = "/tmp/html_etl_check_#{hostname}.#{$$}.tmp"
      begin
        if Hdfs.exists?(tmpfile)
          Hdfs.delete(tmpfile)
        end
        Hdfs::File.open(tmpfile, "w") do |io|
          io.print "test\n"
        end
        return true
      rescue
        $log.error("hdfs check failed..")
        return false
      ensure
        begin
          if Hdfs.exists?(tmpfile)
            Hdfs.delete(tmpfile)
          end
        rescue
          $log.error("hdfs check failed..")
          return false
        end
      end
    end
    
  end
end
