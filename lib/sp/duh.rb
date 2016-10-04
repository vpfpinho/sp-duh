require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'version'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'engine'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'exceptions'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'repl'))

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
# PG Adapters
require 'pg'
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'adapters', 'pg', 'text_decoder', 'json'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'adapters', 'pg', 'text_encoder', 'json'))

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'jsonapi', 'model', 'base'))

# Sharding library classes

require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'exceptions'))
# Sharder classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'sharded_namespace'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'sharder'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'sharding', 'schema_sharder'))

# Migrations library classes
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'migrations'))
require File.expand_path(File.join(File.dirname(__FILE__), 'duh', 'migrations', 'migrator'))

module SP
  module Duh
    def self.root
      File.expand_path '../../..', __FILE__
    end

  end
end

def _log(message, prefix = nil)
  message = message.is_a?(String) ? message : message.inspect
  prefix = "SP::Duh#{prefix.blank? ? '' : ' [' + prefix + ']'}: "
  if Rails.logger && !defined?(Rails::Console)
    Rails.logger.debug "#{prefix}#{message}"
  else
    puts "#{prefix}#{message}"
  end
end

# Configure the I18n module for correct usage when outside a Rails app (tests)
I18n.load_path += Dir[File.join(SP::Duh.root, 'config', 'locales', '*.{rb,yml}')]
I18n.default_locale = :pt
