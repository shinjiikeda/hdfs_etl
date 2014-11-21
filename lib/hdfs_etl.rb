
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
      messages=nil
      
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
          is_success = false
          3.times.each do | n |
            begin
              $log.info("retry..") if n > 0
              Hdfs::File.open(path, mode) do | io |
                io.write values
              end
              current_filesize = Hdfs.size(path)
              $log.info("part: #{part_no}, path: #{path} size: #{current_filesize} success")
              is_successs = true
              break
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
              else
                $log.error "class: #{e.class}, message: #{e.to_s}"
                $log.error e.backtrace
                current_filesize = Hdfs.size(path)
                $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
                $log.error("filesize old: #{filesize}, current: #{current_filesize}") if filesize != current_filesize
              end
            end
            sleep 10
          end
          if ! is_success
            $log.error("part: #{part_no}, path: #{path} size: #{current_filesize} failure!")
            raise BackendError, "part: #{part_no}, path: #{path} failure!"
          end
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
  
    def test_hdfs()
      hostname = `hostname`.chomp!
      tmpfile = "/tmp/medjed_hlog_etl_check_#{hostname}.#{$$}.tmp"
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
    
    def test_hdfs()
      hostname = `hostname`.chomp!
      tmpfile = "/tmp/medjed_hlog_etl_check_#{hostname}.#{$$}.tmp"
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
