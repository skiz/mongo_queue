class Mongo::Queue
  attr_reader :connection, :config
  
  DEFAULT_CONFIG = {
    :database   => 'mongo_queue',
    :collection => 'mongo_queue',
    :timeout    => 300,
    :attempts   => 3
  }.freeze
  
  DEFAULT_INSERT = {
    :priority   => 0,
    :attempts   => 0,
    :locked_by  => nil,
    :locked_at  => nil,
    :last_error => nil
  }.freeze
  
  # Create a new instance of MongoQueue with the provided mongodb connection and optional configuration.
  # See +DEFAULT_CONFIG+ for default configuration and possible configuration options.
  #
  # Example:
  #    db = Mongo::Connection.new('localhost')
  #    config = {:timeout => 90, :attempts => 2}
  #    queue = Mongo::Queue.new(db, config)
  #
  def initialize(connection, opts={})
    @connection = connection
    @config = DEFAULT_CONFIG.merge(opts)
  end
  
  # Remove all items from the queue. Use with caution!
  def flush!
    collection.drop
  end
  
  # Insert a new item in to the queue with required queue message parameters.
  #
  # Example:
  #    queue.insert(:name => 'Billy', :email => 'billy@example.com', :message => 'Here is the thing you asked for')
  def insert(hash)
    id = collection.insert DEFAULT_INSERT.merge(hash)
    collection.find_one(:_id => BSON::ObjectId.from_string(id.to_s))
  end
  
  # Lock and return the next queue message if one is available. Returns nil if none are available. Be sure to
  # review the README.rdoc regarding proper usage of the locking process identifier (locked_by).
  # Example:
  #    locked_doc = queue.lock_next(Thread.current.object_id)
  def lock_next(locked_by)
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {'$set' => {:locked_by => locked_by, :locked_at => Time.now.utc}} 
    cmd['query']         = {:locked_by => nil, :locked_by => nil, :attempts => {'$lt' => @config[:attempts]}}
    cmd['sort']          = sort_hash
    cmd['limit']         = 1
    cmd['new']           = true
    value_of collection.db.command(cmd)
  end
  
  # Removes stale locks that have exceeded the timeout and places them back in the queue.
  def cleanup!
    cursor = collection.find({:locked_by => /.*/, :locked_at => {'$lt' => Time.now.utc - config[:timeout]}})
    doc = cursor.next_document
    while doc
      release(doc, doc['locked_by'])
      doc = cursor.next_document
    end
  end
  
  # Release a lock on the specified document and allow it to become available again.
  def release(doc, locked_by)
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = @config[:collection]
    cmd['update']        = {'$set' => {:locked_by => nil, :locked_at => nil}}
    cmd['query']         = {:locked_by => locked_by, :_id => BSON::ObjectId.from_string(doc['_id'].to_s)}
    cmd['limit']         = 1
    cmd['new']           = true
    value_of collection.db.command(cmd)    
  end

  # Remove the document from the queue. This should be called when the work is done and the document is no longer needed.
  # You must provide the process identifier that the document was locked with to complete it.
  def complete(doc, locked_by)
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = @config[:collection]
    cmd['query']         = {:locked_by => locked_by, :_id => BSON::ObjectId.from_string(doc['_id'].to_s)}
    cmd['remove']        = true
    cmd['limit']         = 1
    value_of collection.db.command(cmd)    
  end
 
  # Increase the error count on the locked document and release. Optionally provide an error message.
  def error(doc, error_message=nil)
    doc['attempts'] +=1
    collection.save doc.merge({
      'last_error' => error_message,
      'locked_by'  => nil,
      'locked_at'  => nil
    })
  end 
  
  # Provides some information about what is in the queue. We are using an eval to ensure that a
  # lock is obtained during the execution of this query so that the results are not skewed.
  # please be aware that it will lock the database during the execution, so avoid using it too
  # often, even though it it very tiny and should be relatively fast.
  def stats
    js = "function queue_stat(){
              return db.eval(
              function(){
                var a = db.#{config[:collection]}.count({'locked_by': null, 'attempts': {$lt: #{config[:attempts]}}});
                var l = db.#{config[:collection]}.count({'locked_by': /.*/});
                var e = db.#{config[:collection]}.count({'attempts': {$gte: #{config[:attempts]}}});
                var t = db.#{config[:collection]}.count();
                return [a, l, e, t];
              }
            );
          }"
    available, locked, errors, total = collection.db.eval(js)
    { :locked    => locked.to_i,
      :errors    => errors.to_i,
      :available => available.to_i,
      :total     => total.to_i }
  end
   
  
  protected
  
  def sort_hash #:nodoc:
    sh = BSON::OrderedHash.new
    sh['priority'] = -1 ; sh
  end
  
  def value_of(result) #:nodoc:
    result['okay'] == 0 ? nil : result['value']
  end
  
  def collection #:nodoc:
    @connection.db(@config[:database]).collection(@config[:collection])
  end
end
