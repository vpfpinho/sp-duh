require 'sp/duh/jsonapi/model/concerns/model'

module SP
  module Duh
    module JSONAPI
      module Model

        class Base
          include ::ActiveRecord::Validations
          include Concerns::Model

          def self.inherited(child)
            child.resource_name = child.name.demodulize.underscore.pluralize
            child.autogenerated_id = true
          end

          def self.i18n_scope
            :activerecord
          end
        end

      end
    end
  end
end
