def require_silently(require_name)
  begin
    require require_name
  rescue LoadError
  end
end

require_silently 'byebug'
require_silently 'awesome_print'

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'version'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'engine'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'exceptions'))

# JSONAPI library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'resource_publisher'))
# Service classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'service'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'configuration'))
# Adpater classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'base'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'raw_db'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'db'))

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'model', 'base'))

# Sharding library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'exceptions'))
# Sharder classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'sharded_namespace'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'sharder'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'schema_sharder'))

module SP
  module Duh
    def self.root
      File.expand_path '../../..', __FILE__
    end
  end
end

def _log(message)
  Rails.logger.debug "SP::Duh: #{message}"
end
