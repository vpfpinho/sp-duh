#!/usr/bin/env rake
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
require 'bundler/gem_tasks'
require 'sp-duh'

load 'lib/tasks/db_utils.rake'
load 'lib/tasks/i18n.rake'

# Allow calling common tasks either from the app (using its environment) or locally from the gem
# In this case, open the connection to the database
task :environment => :pg_connect do
end
