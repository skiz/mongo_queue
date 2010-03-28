require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Mongo::Queue do
  
  before(:suite) do
    opts   = {
      :database   => 'mongo_queue_spec',
      :collection => 'spec',
      :attempts   => 4,
      :timeout    => 60}
    @@db = Mongo::Connection.new('localhost', nil, :pool_size => 4)
    @@queue = Mongo::Queue.new(@@db, opts)
  end
  
  before(:each) do
    @@queue.flush!
  end
  
  describe "Configuration" do

    it "should set the connection" do
      @@queue.connection.should be(@@db)
    end

    it "should allow database option" do
      @@queue.config[:database].should eql('mongo_queue_spec')
    end
    
    it "should allow collection option" do
      @@queue.config[:collection].should eql('spec')
    end

    it "should allow attempts option" do
      @@queue.config[:attempts].should eql(4)
    end
  
    it "should allow timeout option" do
      @@queue.config[:timeout].should eql(60)
    end
  
    it "should have a sane set of defaults" do
      q = Mongo::Queue.new(nil)
      q.config[:collection].should eql 'mongo_queue'
      q.config[:attempts].should   eql 3
      q.config[:timeout].should    eql 300
    end
  end

  describe "Inserting a Job" do
    before(:each) do
      @@queue.insert(:message => 'MongoQueueSpec')
      @item = @@queue.send(:collection).find_one  
    end
    
    it "should set priority to 0 by default" do
      @item['priority'].should be(0)
    end
    
    it "should set a null locked_by" do
      @item['locked_by'].should be(nil)      
    end
    
    it "should set a null locked_at" do
      @item['locked_at'].should be(nil)
    end
    
    it "should allow additional fields" do
      @item['message'].should eql('MongoQueueSpec')
    end
    
    it "should set a blank last_error" do
      @item['last_error'].should be(nil)
    end
  end
    
  describe "Working with the queue" do
    before(:each) do
      @first  = @@queue.insert(:msg => 'First')
      @second = @@queue.insert(:msg => 'Second', :priority => 2)
      @third  = @@queue.insert(:msg => 'Third',  :priority => 6)
      @fourth = @@queue.insert(:msg => 'Fourth', :locked_by => 'Example', :locked_at => Time.now.utc - 60 * 60 * 60, :priority => 99)
    end
    
    it "should lock the next document by priority" do
      doc = @@queue.lock_next('Test')
      doc['msg'].should eql('Third')
    end
    
    it "should release and relock the next document" do
      @@queue.release(@fourth, 'Example')
      @@queue.lock_next('Bob')['msg'].should eql('Fourth')
    end
    
    it "should remove completed items" do
      doc = @@queue.lock_next('grr')
      @@queue.complete(doc,'grr')
      @@queue.lock_next('grr')['msg'].should eql('Second')
    end
    
    it "should return nil when unable to lock" do
      4.times{ @@queue.lock_next('blah') }
      @@queue.lock_next('blah').should eql(nil)
    end
  end
  
  describe "Error Handling" do
    it "should allow document error handling" do
      doc = @@queue.insert(:stuff => 'Broken')
      2.times{ @@queue.error(doc, 'I think I broke it') }
      doc = @@queue.lock_next('Money')
      doc['attempts'].should eql(2)
      doc['last_error'].should eql('I think I broke it')
    end
  end
  
  describe "Cleaning up" do
    it "should remove all of the stale locks" do
      @@queue.insert(:msg => 'Fourth', :locked_by => 'Example', :locked_at => Time.now.utc - 60 * 60 * 60, :priority => 99)
      @@queue.cleanup!
      @@queue.lock_next('Foo')['msg'].should eql('Fourth')
    end
  end
    
end