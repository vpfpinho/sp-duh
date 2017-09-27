module SP
  module Duh
    module JSONAPI
      class Parameters
        attr_reader :user_id, :company_id, :company_schema, :sharded_schema, :default_accounting_schema, :accounting_schema, :accounting_prefix

        def initialize(parameters = {})
          check_jsonapi_args(parameters)

          @user_id                   = parameters[:user_id].to_s              unless parameters[:user_id].nil?
          @company_id                = parameters[:company_id].to_s           unless parameters[:company_id].nil?
          @company_schema            = parameters[:company_schema]            unless parameters[:company_schema].nil?
          @sharded_schema            = parameters[:sharded_schema]            unless parameters[:sharded_schema].nil?
          @default_accounting_schema = parameters[:default_accounting_schema] unless parameters[:default_accounting_schema].nil?
          @accounting_schema         = parameters[:accounting_schema]         unless parameters[:accounting_schema].nil?
          @accounting_prefix         = parameters[:accounting_prefix]         unless parameters[:accounting_prefix].nil?
        end

        def to_json(options = {})
          {
            user_id: self.user_id,
            company_id: self.company_id,
            company_schema: self.company_schema,
            sharded_schema: self.sharded_schema,
            default_accounting_schema: self.default_accounting_schema,
            accounting_schema: self.accounting_schema,
            accounting_prefix: self.accounting_prefix
          }.to_json
        end

        private
        def check_jsonapi_args(parameters)
          if parameters.keys.any? && !(parameters.keys - valid_keys).empty?
            raise SP::Duh::JSONAPI::Exceptions::InvalidJSONAPIKeyError.new(key: (parameters.keys - valid_keys).join(', '))
          end
        end

        def valid_keys
          [ :user_id, :company_id, :company_schema, :sharded_schema, :default_accounting_schema, :accounting_schema, :accounting_prefix ]
        end
      end
    end
  end
end
