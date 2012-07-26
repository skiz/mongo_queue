# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ["Josh Martin"]
  gem.email         = %q{jmartin@webwideconsulting.com}
  gem.description   = %q{An extensible thread safe job/message queueing system that uses mongodb as the persistent storage engine.}
  gem.summary       = %q{Mongo based message/job queue}
  gem.homepage      = %q{http://github.com/skiz/mongo_queue}
  gem.date          = %q{2010-03-30}

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = %q{mongo_queue}
  gem.require_paths = ["lib"]
  gem.version       = '0.1.3'
  gem.add_development_dependency("rspec", ">= 0")
  gem.add_development_dependency("rdoc", ">= 0")
  gem.add_dependency("mongo", ">= 1.5")
  gem.add_dependency("bson_ext", ">= 1.5")
 
  
  gem.rdoc_options = ["--charset=UTF-8"]
  gem.extra_rdoc_files = [
    "LICENSE",
    "README.rdoc"
  ]
end

