require 'mongo'

begin
  require 'yajl'
rescue LoadError
  require 'json'
end

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/helpers'
require 'resque/stat'
require 'resque/job'
require 'resque/worker'

module Resque
  include Helpers
  extend self

  # Accepts a 'hostname:port' string or a Redis server.
  def mongo=(server)
    case server
    when String
      host, port = server.split(':')
      @con = Mongo::Connection.new(host, port)
      @db = @con.db('monque')
      @mongo = @db.collection('monque')
      @workers = @db.collection('workers')
      @failures = @db.collection('failures')
      @stats = @db.collection('stats')

      add_indexes
    else
      raise "I don't know what to do with #{server.inspect}"
    end
  end


  # Returns the current Redis connection. If none has been created, will
  # create a new one.
  def mongo
    return @mongo if @mongo
    self.mongo = 'localhost:27017'
    self.mongo
  end

  def mongo_workers
    return @workers if @workers
    self.mongo = 'localhost:27017'
    @workers
  end

  def mongo_failures
    return @failures if @failures
    self.mongo = 'localhost:27017'
    @failures
  end

  def mongo_stats
    return @stats if @stats
    self.mongo = 'localhost:27017'
    @stats
  end
  
  def to_s
    "Mongo Client connected to #{@con.host}"
  end

  def add_indexes
    @mongo.create_index :queue
    @workers.create_index :worker
    @stats.create_index :stat
  end

  def drop
    @mongo.drop
    @workers.drop
    @failures.drop
    @stats.drop
    @mongo = nil
  end
  
  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  def push(queue, item)
    watch_queue(queue)
    mongo << { :queue => queue.to_s, :item => encode(item) }
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    doc = mongo.find_modify( :query => { :queue => queue },
                             :sort => [:natural, :desc],
                             :remove => true )
    decode doc['item']
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an int representing the size of a queue.
  # Queue name should be a string.
  def size(queue)
    mongo.find(:queue => queue).count
  end

  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1)
    res = mongo.find(:queue => queue).sort([:natural, :desc]).skip(start).limit(count).to_a
    res.collect! { |doc| decode(doc['item']) }
    
    if count == 1
      return nil if res.empty?
      res.first
    else
      return [] if res.empty?
      res
    end
  end

  # Returns an array of all known Resque queues as strings.
  def queues
    mongo.distinct(:queue)
  end
  
  # Given a queue name, completely deletes the queue.
  def remove_queue(queue)
    mongo.remove(:queue => queue)
  end

  # Used internally to keep track of which queues we've created.
  # Don't call this directly.
  def watch_queue(queue)
#    redis.sadd(:queues, queue.to_s)
  end


  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will return a non-true value.
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    queue = klass.instance_variable_get(:@queue)
    queue ||= klass.queue if klass.respond_to?(:queue)
    Job.create(queue, klass, *args)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end


  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end


  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.size,
      :failed    => Stat[:failed],
      :servers   => ["#{@con.host}:#{@con.port}"]
    }
  end

  # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  # is O(N) for the keyspace, so be careful - this can be slow for big databases.
  def keys
    queues
  end
end
