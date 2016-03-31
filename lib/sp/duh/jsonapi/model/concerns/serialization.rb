module SP
  module Duh
    module JSONAPI
      module Model
        module Concerns
          module Serialization
            extend ::ActiveSupport::Concern

            included do
            end

            def as_json(options = {})
              root = ActiveRecord::Base.include_root_in_json
              root = options[:root] if options.try(:key?, :root)
              if root
                root = self.class.name.underscore.gsub('/','_').to_sym
                { root => serializable_hash(options) }
              else
                serializable_hash(options)
              end
            end

            def from_json(json)
              root = ActiveRecord::Base.include_root_in_json
              hash = ActiveSupport::JSON.decode(json)
              hash = hash.values.first if root
              self.attributes = hash
              self
            end

          private

            alias :read_attribute_for_serialization :send

            def serializable_hash(options = {})

              attribute_names = self.class.attributes.sort
              if only = options[:only]
                attribute_names &= Array.wrap(only).map(&:to_s)
              elsif except = options[:except]
                attribute_names -= Array.wrap(except).map(&:to_s)
              end

              hash = {}
              attribute_names.each { |n| hash[n] = read_attribute_for_serialization(n) }

              method_names = Array.wrap(options[:methods]).select { |n| respond_to?(n) }
              method_names.each { |n| hash[n] = send(n) }

              hash

            end

          end
        end
      end
    end
  end
end
