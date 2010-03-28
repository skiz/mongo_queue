require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe MongoQueue do
  
  before(:each) do
    @mc = mock('MongoCollection')
    @db = mock('MongoDatabase',:collection => @mc)
    @cp = mock('MongoConnectionPool', :db => @db)
    @queue = MongoQueue.new(@cp)
  end
  
  describe "Configuration" do
    before(:each) do
      opts = {
        :database   => 'spec_db',
        :collection => 'spec_co',
        :attempts   => 99,
        :timeout    => 10
      }
      @queue = MongoQueue.new(@cp,opts)
    end

    it "should set the connection" do
      @queue.connection.should be(@cp)
    end
    
    it "should have a proper connection reference" do
      @queue.send(:collection).should be(@mc)
    end
    
    it "should have a sane set of defaults" do
      queue = MongoQueue.new(@cp)
      queue.config.should eql({
        :database   => 'mongo_queue' ,
        :collection => 'mongo_queue',
        :attempts   => 3,
        :timeout    => 18000
      })
    end
    
    it "should allow database option" do
      @queue.config[:database].should eql('spec_db')
    end
    
    it "should allow collection option" do
      @queue.config[:collection].should eql('spec_co')
    end

    it "should allow attempts option" do
      @queue.config[:attempts].should eql(99)
    end
    
    it "should allow timeout option" do
      @queue.config[:timeout].should eql(10)
    end

  end
  
  describe "Inserting a Job" do
    it "should provide method to insert items with proper elements" do
      @mc.should_receive(:insert).with({
        :message    => 'MongoQueueSpec',
        :priority   => 0,
        :attempts   => 0,
        :locked_by  => nil,
        :locked_at  => nil,
        :last_error => nil
      })
      @queue.insert(:message => 'MongoQueueSpec')
    end
  end
  
  describe "Locking a Job" do
  end
  
  describe "Unlocking a Job" do
  end
  
  describe "Worker Timeout" do
  end
  
  describe "Error Handling" do
  end
  
  describe "Removing Locks" do
  end
  
  
  
  # before(:each) do
  #   @klass = Crawler::MongoQueue
  #   @mc = mock('Collection')
  #   @db = mock('MongoDb',:collection => @mc)
  #   @cp = mock('MongoConnectionPool', :db => @db)
  #   Crawler.stub!(:mongo_connection_pool).and_return(@cp)
  #   @instance = Crawler::MongoQueue.new('test')
  #   @queue_item = Hash.new(:url => 'http://example.com')
  # end
  # 
  # describe "standard settings" do
  #   it "should have a defined MAX_ATTEMPTS" do
  #     @klass::MAX_ATTEMPTS.should eql(3)
  #   end
  #   
  #   it "should have a lock timeout" do
  #     @klass::MAX_TIMEOUT.should eql(60 * 60 * 5)
  #   end
  # end
  # 
  # describe "instantiating a queue" do
  #   it "should require and set a collection name" do
  #     qn = 'my_queue_name'
  #     q = @klass.new(qn)
  #     q.collection_name.should eql(qn)
  #   end
  # end
  # 
  # describe "obtaining a mongo connection" do
  #   it "should properly provide a collection" do
  #     q = @klass.new('test')
  #     @db.should_receive(:collection).with('test').and_return(@mc)
  #     q.send(:collection).should eql(@mc)
  #   end
  # end
  # 
  # describe "retrieving a record" do
  #   it "should provide a find_next" do
  #     @instance.should respond_to(:find_next)      
  #   end
  #   
  #   it "should look for a queue item within the range required" do
  #     @instance.should_receive(:clear_stale_locks)
  #     @mc.should_receive(:find_one).with(
  #     { :locked_at => nil, 
  #       :locked_by => nil, 
  #       :attempts  => {'$lt' => @klass::MAX_ATTEMPTS}}, 
  #       :sort      => ['priority','descending']).and_return(@queue_item)
  #     @instance.find_next.should_not be_nil
  #   end
  # end
  # 
  # describe "locking queue items" do
  #   before(:each) do
  #     @mc.stub!(:save)
  #     @mc.stub!(:find_one).and_return(@queue_item)
  #   end
  #   
  #   describe "creating locks" do
  #     it "should set the locked_by and locked_at" do
  #       @instance.lock(@queue_item, 'example')
  #       @queue_item['locked_by'].should eql('example')
  #       @queue_item['locked_at'].should_not be_nil
  #     end
  #     
  #     it "should confirm that it has a lock before saving" do
  #       @mc.should_receive(:save).with(@queue_item)
  #       @instance.lock(@queue_item, 'example')
  #     end
  #   end
  # 
  #   describe "checking locks" do
  #     it "should be able to find the currently locked items by locked_by" do
  #       @mc.should_receive(:find_one).with({:locked_by => 'example'})
  #       @instance.current_lock('example')
  #     end
  #   
  #     it "should be able to verify a lock by locked_by" do
  #       @queue_item['locked_by'] = 'example'
  #       @instance.verify_lock(@queue_item, 'example').should be_true
  #       @instance.verify_lock(@queue_item, 'invalid').should be_false
  #     end
  #   end
  # 
  #   describe "removing locks" do
  #     it "should have the ability to clear all stale locks" do
  #       mock_doc = @queue_item
  #       mock_cursor = mock('MongoCursor')
  #       mock_cursor.stub!(:next_document) do
  #         mock_doc
  #         mock_cursor.stub!(:next_document).and_return nil
  #       end
  #       Time.stub!(:now).and_return(999)
  #       @mc.should_receive(:find).with({:locked_by => /.*/, :locked_at => {'$gt' => 999 - @klass::MAX_TIMEOUT}}).and_return(mock_cursor)
  #       @instance.should_receive(:release)
  #       @instance.clear_stale_locks
  #     end
  #     
  #     it "should be able to release a lock directly with proper locked_by"
  #     it "should provide a way to remove completed queue items"
  #   end
  #   
  # end
  # 
  # describe "error handling" do
  #   it "should update attempts when failed"
  #   it "should store the last error"
  #   it "should release any lock"
  # end
  # 
  # 
end