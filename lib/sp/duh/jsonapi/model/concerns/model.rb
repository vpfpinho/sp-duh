require 'active_support'
require 'active_record'

require 'sp/duh/jsonapi/model/concerns/attributes'
require 'sp/duh/jsonapi/model/concerns/serialization'
require 'sp/duh/jsonapi/model/concerns/persistence'

module SP
  module Duh
    module JSONAPI
      module Model
        module Concerns
          module Model
            extend ::ActiveSupport::Concern

            include Attributes
            include Serialization
            include Persistence

            included do
            end

            module ClassMethods

              def inspect
                "#{super}(#{self.attributes.join(', ')})"
              end
            end

            # Returns the contents of the record as a nicely formatted string.
            def inspect
              # attrs = self.class.attributes
              inspection = self.class.attributes.collect { |name| "#{name}: #{attribute_for_inspect(name)}" }.compact.join(", ")
              "#<#{self.class} #{inspection}>"
            end

          end
        end
      end
    end
  end
end
