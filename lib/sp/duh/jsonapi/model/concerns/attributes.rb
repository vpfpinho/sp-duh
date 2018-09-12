module SP
  module Duh
    module JSONAPI
      module Model
        module Concerns
          module Attributes
            extend ::ActiveSupport::Concern

            included do
            end

            module ClassMethods

              def attributes
                if !@attributes && superclass.respond_to?(:attributes)
                  @attributes = []
                  @attributes += superclass.attributes
                end
                @attributes = [] if !@attributes
                @attributes
              end

              def attr_accessible(name)
                attributes << name if !attributes.include?(name)
                attr_accessor name
              end
            end

            def initialize(new_attributes = nil)
              assign_attributes(new_attributes) if new_attributes
              yield self if block_given?
            end

            def attributes=(new_attributes)
              return unless new_attributes.is_a?(Hash)
              assign_attributes(new_attributes)
            end

            private

              def assign_attributes(new_attributes)
                return if new_attributes.blank?

                new_attributes = new_attributes.stringify_keys
                nested_parameter_attributes = []

                new_attributes.each do |k, v|
                  if respond_to?("#{k}=")
                    if v.is_a?(Hash)
                      nested_parameter_attributes << [ k, v ]
                    else
                      send("#{k}=", v)
                    end
                  # else
                  #   raise(ActiveRecord::UnknownAttributeError, "unknown attribute: #{k}")
                  end
                end

                # Assign any deferred nested attributes after the base attributes have been set
                nested_parameter_attributes.each do |k,v|
                  send("#{k}=", v)
                end
              end

              # Returns an <tt>#inspect</tt>-like string for the value of the
              # attribute +attr_name+. String attributes are truncated upto 50
              # characters, and Date and Time attributes are returned in the
              # <tt>:db</tt> format. Other attributes return the value of
              # <tt>#inspect</tt> without modification.
              #
              #   person = Person.create!(:name => "David Heinemeier Hansson " * 3)
              #
              #   person.attribute_for_inspect(:name)
              #   # => '"David Heinemeier Hansson David Heinemeier Hansson D..."'
              #
              #   person.attribute_for_inspect(:created_at)
              #   # => '"2009-01-12 04:48:57"'
              def attribute_for_inspect(name)
                value = self.send(name)
                if value.is_a?(String) && value.length > 50
                  "#{value[0..50]}...".inspect
                elsif value.is_a?(Date) || value.is_a?(Time)
                  %("#{value.to_s(:db)}")
                else
                  value.inspect
                end
              end

          end
        end
      end
    end
  end
end
