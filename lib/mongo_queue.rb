class MongoQueue
  attr_reader :connection, :config
  
  DEFAULT_CONFIG = {
    :database   => 'mongo_queue',
    :collection => 'mongo_queue',
    :timeout    => 18000,
    :attempts   => 3
  }.freeze
  
  DEFAULT_INSERT = {
    :priority   => 0,
    :attempts   => 0,
    :locked_by  => nil,
    :locked_at  => nil,
    :last_error => nil
  }.freeze
  
  # Create a new instance of MongoQueue with the provided mongodb connection and optional configuration
  def initialize(connection, opts={})
    @connection = connection
    @config = DEFAULT_CONFIG.merge(opts)
  end
  
  # Insert a new item in to the queue with required queue message parameters.
  def insert(hash)
    collection.insert DEFAULT_INSERT.merge(hash)
  end
  
  # Lock and return the next queue message
  def lock_next(doc, locked_by)
    collection.find_one({:locked_at => nil, :locked_by => nil, :attempts => {'$lt' => MAX_ATTEMPTS}}, :sort => ['priority','descending'])
  end
  
  
  # Lock a document 
  def lock(doc, locked_by)
    doc['locked_by'] = locked_by
    doc['locked_at'] = Time.now
    collection.save(doc)
  end
  
  # # find the next available lockable item
  # def find_next
  #   clear_stale_locks
  #   collection.find_one({:locked_at => nil, :locked_by => nil, :attempts => {'$lt' => MAX_ATTEMPTS}}, :sort => ['priority','descending'])
  # end
  # 
  
  # Removes stale locks that have exceeded the timeout, and put them back in the queue.
  def cleanup
    cursor = collection.find({:locked_by => /.*/, :locked_at => {'$gt' => Time.now - MAX_TIMEOUT}})
    doc = cursor.next_document
    while doc
      release(doc, doc['locked_by'])
      doc = cursor.next_document
    end
  end
  # 
  # # attempt to lock an item in the queue for processing
  # def lock(doc, locked_by)
  #   return false unless doc
  #   doc['locked_by'] = locked_by
  #   doc['locked_at'] = Time.now
  #   collection.save(doc)
  #   verify_lock(doc, locked_by)
  # end
  # 
  # # returns a currently locked doc
  # def current_lock(locked_by)
  #   collection.find_one({:locked_by => locked_by})
  # end
  # 
  # # release the queue item lock
  # def release(doc, locked_by)
  #   if verify_lock(doc, locked_by) && locked_by == doc['locked_by']
  #     doc['locked_by'] = nil
  #     doc['locked_at'] = nil
  #     collection.save(doc)
  #   end
  # end
  # 
  # # remove the document from the queue
  # def complete(doc, locked_by)
  #   return false unless verify_lock(doc, locked_by)
  #   collection.remove(doc)
  # end
  # 
  # # does the specific locked_by have a lock?
  # def verify_lock(doc, locked_by)
  #   doc['locked_by'] == locked_by ? doc : false
  # end
  # 
  # # increase the errors on the locked doc and release
  # def error(doc, error_message=nil)
  #   doc['attempts'] ||= 0
  #   doc['attempts'] += 1
  #   doc['last_error'] = error_message
  #   release(doc, doc['locked_by'])
  # end
  # 
  
  protected
  
  def collection
    @connection.db(@config[:database]).collection(@config[:collection])
  end
end
