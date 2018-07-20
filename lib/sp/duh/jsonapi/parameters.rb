module SP
  module Duh
    module JSONAPI
      class Parameters
        attr_reader :user_id, :entity_id, :entity_schema, :sharded_schema, :subentity_schema, :subentity_prefix

        def initialize(parameters = {})
          check_jsonapi_args(parameters)

          @user_id          = parameters[:user_id].to_s     unless parameters[:user_id].nil?
          @entity_id        = parameters[:entity_id].to_s   unless parameters[:entity_id].nil?
          @entity_schema    = parameters[:entity_schema]    unless parameters[:entity_schema].nil?
          @sharded_schema   = parameters[:sharded_schema]   unless parameters[:sharded_schema].nil?
          @subentity_schema = parameters[:subentity_schema] unless parameters[:subentity_schema].nil?
          @subentity_prefix = parameters[:subentity_prefix] unless parameters[:subentity_prefix].nil?
        end

        def to_json(options = {})
          {
            user_id: self.user_id,
            entity_id: self.entity_id,
            company_schema: self.entity_schema,
            sharded_schema: self.sharded_schema,
            subentity_schema: self.subentity_schema,
            subentity_prefix: self.subentity_prefix
          }.to_json
        end

        private
        def check_jsonapi_args(parameters)
          if parameters.keys.any? && !(parameters.keys - valid_keys).empty?
            raise SP::Duh::JSONAPI::Exceptions::InvalidJSONAPIKeyError.new(key: (parameters.keys - valid_keys).join(', '))
          end
        end

        def valid_keys
          [ :user_id, :entity_id, :entity_schema, :sharded_schema, :subentity_schema, :subentity_prefix ]
        end
      end

      class ParametersNotPicky < Parameters

        def initialize (parameters)
          @user_id          = parameters[:user_id].to_s
          @entity_id        = parameters[:entity_id].to_s
          @entity_schema    = parameters[:entity_schema]
          @sharded_schema   = parameters[:sharded_schema]
          @subentity_schema = parameters[:subentity_schema]
          @subentity_prefix = parameters[:subentity_prefix]
        end

      end

    end
  end
end
