require 'rails/generators'

module Rails
  module Generators
    class <<self
      alias_method :original_invoke, :invoke
    end

    def self.invoke(namespace, args=ARGV, config={})
      original_invoke namespace.tr('-', '_'), args, config
    end
  end
end