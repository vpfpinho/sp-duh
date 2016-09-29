# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sp/duh/version'

Gem::Specification.new do |spec|
  spec.name          = "sp-duh"
  spec.version       = SP::Duh::VERSION
  spec.authors       = ["Jorge Morais"]
  spec.email         = ["jorge.morais@cldware.com"]
  spec.summary       = %q{Gem to manage JSONAPI requests to a resourceful database}
  spec.description   = %q{Gem to manage JSONAPI requests to a resourceful database}
  spec.homepage      = "https://github.com/vpfpinho/sp-duh"
  spec.license       = "AGPL"

  spec.files         = Dir["{config,lib,test}/**/*"] + ["LICENSE", "README.md", "Rakefile"]
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.post_install_message = <<-'EOF'.gsub(/^ +/, '')
    ****************************
    * FOR CONNECTIONS TO REDIS *
    ****************************

    From this version on, you have the possibility to connect a PostgreSQL database to Redis.
    For that to be possible, the Writable Foreign Data Wrapper for Redis (https://github.com/nahanni/rw_redis_fdw)
    must be installed.

    DEPENDENCIES: hiredis

    On Linux:  sudo apt-get install libhiredis-dev
    On Mac OS: brew install hiredis

    INSTALLATION:

    git clone https://github.com/nahanni/rw_redis_fdw.git
    cd rw_redis_fdw
    PATH=<PostgreSQL binary path>:$PATH make
    sudo PATH=<PostgreSQL binary path>:$PATH make install
  EOF


  spec.add_development_dependency 'awesome_print', '~> 1.6'
  spec.add_development_dependency 'byebug', '~> 8.2'

  spec.add_dependency "rails", "~> 3.2"
end
