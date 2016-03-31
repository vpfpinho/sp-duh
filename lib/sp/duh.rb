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

module SP
  module Duh
  end
end