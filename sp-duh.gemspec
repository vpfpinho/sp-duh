#
# Copyright (c) 2011-2017 Cloudware S.A. All rights reserved.
#
# This file is part of sp-duh.
#
# sp-duh is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sp-duh is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with sp-duh.  If not, see <http://www.gnu.org/licenses/>.
#
# encoding: utf-8
#
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

  spec.add_dependency 'highline'
  spec.add_dependency 'sp-excel-loader'

  spec.add_development_dependency 'awesome_print', '~> 1.6'
  spec.add_development_dependency 'byebug', '~> 8.2'

  spec.add_dependency "rails", "~> 3.2"
end
