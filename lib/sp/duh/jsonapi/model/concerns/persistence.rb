module SP
  module Duh
    module JSONAPI
      module Model
        module Concerns
          module Persistence
            extend ::ActiveSupport::Concern

            included do

              # Define schema and prefix accessors at the class (and subclass) level (static).
              # These attribute values are inherited by subclasses, but can be changed in each subclass without affecting the parent class.
              # Instances can access these attributes at the class level only.
              class_attribute :schema, instance_reader: false, instance_writer: false
              class_attribute :prefix, instance_reader: false, instance_writer: false

              # Idem for data adapter configuration...
              # In a similar way to ActiveRecord::Base.connection, the adapter should be defined at the base level and is inherited by all subclasses
              class_attribute :adapter, instance_reader: false, instance_writer: false

              self.autogenerated_id = true

              attr_accessible :id
            end

            module ClassMethods

              # Define resource configuration accessors at the class (and subclass) level (static).
              # These attribute values are NOT inherited by subclasses, each subclass MUST define their own.
              # Instances can access these attributes at the class level only.
              attr_accessor :resource_name
              attr_accessor :autogenerated_id

              def find!(id, conditions = nil) ; get(id, conditions) ; end

              def find(id, conditions = nil)
                begin
                  get(id, conditions)
                rescue Exception => e
                  return nil
                end
              end

              def query!(condition) ; get_all(condition) ; end

              def query(condition)
                begin
                  get_all(condition)
                rescue Exception => e
                  nil
                end
              end

              def first!(condition = "")
                get_all(condition).first
              end

              def first(condition = "")
                begin
                  get_all(condition).first
                rescue Exception => e
                  nil
                end
              end

              def all! ; get_all("") ; end

              def all
                begin
                  get_all("")
                rescue Exception => e
                  nil
                end
              end

              private

                def get(id, conditions = nil)
                  result = self.adapter.unwrap_request do
                    self.adapter.get(File.join(self.resource_name, id.to_s), self.schema.to_s, self.prefix.to_s, conditions)
                  end
                  jsonapi_result_to_instance(result[:data], result)
                end

                def get_all(condition)
                  got = []
                  result = self.adapter.unwrap_request do
                    self.adapter.get(self.resource_name, self.schema.to_s, self.prefix.to_s, condition)
                  end
                  if result
                    got = result[:data].map do |item|
                      data = { data: item }
                      data.merge(included: result[:included]) if result[:included]
                      jsonapi_result_to_instance(item, data)
                    end
                  end
                  got
                end

                def jsonapi_result_to_instance(result, data)
                  if result
                    instance = self.new(result.merge(result[:attributes]).except(:attributes))
                    instance.send :_data=, data
                  end
                  instance
                end
            end

            # Instance methods

            def new_record?
              if self.class.autogenerated_id || self.id.nil?
                self.id.nil?
              else
                self.class.find(self.id).nil?
              end
            end

            def save!
              if new_record?
                create!
              else
                update!
              end
            end

            def destroy!
              if !new_record?
                self.class.adapter.delete(path_for_id, self.class.schema.to_s, self.class.prefix.to_s)
              end
            end

            alias :delete! :destroy!

            def create!
              if self.class.autogenerated_id
                params = {
                  data: {
                    type: self.class.resource_name,
                    attributes: get_persistent_json.reject { |k,v| k == :id || v.nil? }
                  }
                }
              else
                params = {
                  data: {
                    type: self.class.resource_name,
                    attributes: get_persistent_json.reject { |k,v| v.nil? }
                  }
                }
              end
              result = self.class.adapter.unwrap_request do
                self.class.adapter.post(self.class.resource_name, self.class.schema.to_s, self.class.prefix.to_s, params)
              end
              # Set the id to the newly created id
              self.id = result[:data][:id]
            end

            def update!
              params = {
                data: {
                  type: self.class.resource_name,
                  id: self.id.to_s,
                  attributes: get_persistent_json.reject { |k,v| k == :id }
                }
              }
              result = self.class.adapter.unwrap_request do
                self.class.adapter.patch(path_for_id, self.class.schema.to_s, self.class.prefix.to_s, params)
              end
            end

            def get_persistent_json
              as_json.reject { |k| !k.in?(self.class.attributes) }
            end

            protected

              attr_accessor :_data

            private

              def path_for_id ; File.join(self.class.resource_name, self.id.to_s) ; end

          end
        end
      end
    end
  end
end
