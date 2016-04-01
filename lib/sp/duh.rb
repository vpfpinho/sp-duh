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

# JSONAPI library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'exceptions'))
# Service classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'service'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'configuration'))
# Adpater classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'base'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'adapters', 'db'))

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'model', 'base'))

module SP
  module Duh
  end
end

def _log(message)
  Rails.logger.debug "SP::Duh: #{message}"
end
