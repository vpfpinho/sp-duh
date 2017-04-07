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

  spec.add_development_dependency 'awesome_print', '~> 1.6'
  spec.add_development_dependency 'byebug', '~> 8.2'

  spec.add_dependency "rails", "~> 3.2"
end
