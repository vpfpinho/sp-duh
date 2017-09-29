#!/usr/bin/env rake

require 'sp-duh'

load 'lib/tasks/db_utils.rake'
load 'lib/tasks/i18n.rake'
load 'lib/tasks/xls2jrxml.rake'

# Allow calling common tasks either from the app (using its environment) or locally from the gem
# In this case, open the connection to the database
task :environment => :pg_connect do
end
