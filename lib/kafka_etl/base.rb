
require 'poseidon'
require 'zk'
require 'parallel'
require 'logger'

module KafkaETL
  class BackendError < StandardError; end
  class Base
    def initialize(zookeeper, kafka_brokers, kafka_topic, opts={})
      
      @num_threads = opts[:num_threads] ? opts[:num_threads] : 2 
      @max_fetch_bytes = opts[:max_fetch_bytes] ? opts[:max_fetch_bytes] : 20_000_000
      @kafka_clint_id = opts[:kafka_client_id] ? opts[:kafka_client_id] : "my_consumer"
      @kafka_part_num = opts[:kafka_topic_part_num] ? opts[:kafka_topic_part_num] : 4
      
      @zookeeper = zookeeper
      @kafka_brokers = kafka_brokers
      @kafka_topic = kafka_topic
      
      $log = Logger.new(STDOUT) if $log.nil?

      $log.debug("zookeeper: #{@zookeeper}")
      $log.debug("kafka_brokers: #{@kafka_brokers}")
      $log.debug("kafka_topic: #{@kafka_topic}")
      
      @partition_shuffle = true
      
      @mutex = Mutex.new
      
      @total_procs = 0
    end
    
    def process()
      
      @total_procs = 0
      
      zk = ZK.new(@zookeeper)
      zk.create("/", ignore: :node_exists)
      
      begin
        seq = [ * 0 ... @kafka_part_num ]
        seq.shuffle! if @partition_shuffle == true
        
        r = Parallel.each(seq, :in_threads => @num_threads) do |part_no|
          zk_lock = "lock_hdfs_part_#{part_no}"
          zk.with_lock(zk_lock) do
            proccess_thread(zk, part_no)
          end
        end
      ensure
        zk.close
      end
      $log.info "total procs: #{@total_procs}"
    end
    
    def process_partition(part_no)
      zk = ZK.new(@zookeeper)
      begin
        zk_lock = "lock_hdfs_part_#{part_no}"
        locker = zk.locker(zk_lock)
        begin
          if locker.lock!
            proccess_thread(zk, part_no)
          else
            $log.info("part: #{part_no} is aleady locked skip")
          end
        ensure
          locker.unlock!
        end
      ensure
        zk.close
      end
    end
    
    def proccess_thread(zk, part_no)
      zk_part_node = "/part_offset_#{part_no}"
      
      num_cur_part_procs = 0
      
      if ! zk.exists?(zk_part_node)
        zk.create(zk_part_node, "0")
      end
      
      offset = nil
      begin
        value, stat = zk.get(zk_part_node)
        offset = value.to_i
        if offset == 0
          part_offset = :earliest_offset
          #part_offset = :latest_offset
        else
          part_offset = offset
        end
      rescue ZK::Exceptions::NoNode => e
        part_offset = :earliest_offset
        #part_offset = :latest_offset
      end
      $log.debug "part: #{part_no}, offset: #{part_offset}"
      
      cons = Poseidon::PartitionConsumer.consumer_for_partition(@kafka_client_id,
                                                                @kafka_brokers,
                                                                @kafka_topic,
                                                                part_no,
                                                                part_offset,
                                                                :max_wait_ms => 0,
                                                                :max_bytes => @max_fetch_bytes)
      begin
        num_cur_part_procs += process_messages(cons, part_no)
        
        next_offset = cons.next_offset
        last_offset = cons.highwater_mark
        $log.info "part: #{part_no}, next_offset: #{next_offset}, last_offset: #{last_offset},  proc: #{num_cur_part_procs}"
        @mutex.synchronize do
          @total_procs += num_cur_part_procs
        end
        # set next offset to zookeper
        zk.set(zk_part_node, next_offset.to_s) if next_offset >= offset
      rescue Poseidon::Errors::NotLeaderForPartition => e
        $log.error "Skip: Not Leader For Partition"
      rescue Poseidon::Errors::OffsetOutOfRange => e
        $log.error e.to_s
        zk.set(zk_part_node, "0")
      #rescue NoMethodError => e
      #  next_offset = cons.next_offset
      #  zk.set(zk_part_node, next_offset.to_s)
      #  $log.error e.to_s
      rescue Poseidon::Connection::ConnectionFailedError
        $log.error "kafka connection failed"
      rescue BackendError
        $log.debug "backend error"
        #raise e
      rescue => e
        raise e
      ensure
        cons.close if ! cons.nil?
      end
    rescue Poseidon::Errors::OffsetOutOfRange => e
      $log.error e.to_s
      zk.set(zk_part_node, "0")
    end
    
    def process_messages(cons)
      messages = cons.fetch
      messages.each do |m|
        key = m.key
        val = m.value
        puts "key: #{key}, val: #{val}"
      end
    end
  end
end
