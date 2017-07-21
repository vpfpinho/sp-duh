module SP
  module Duh
    module JSONAPI
      module Adapters

        class RawDb < Base

          protected

            def unwrap_response(response)
              status = response[0].to_i
              result = response[1]
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if status != SP::Duh::JSONAPI::Status::OK
              result
            end

            def get_error_response(path, error) ; error_response(path, error).to_json ; end

            def do_request(method, path, params, jsonapi_args)
              result = do_request_on_the_db(method, path, params, jsonapi_args)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [
                SP::Duh::JSONAPI::Status::OK,
                result
              ]
            end

          private
            def valid_keys
              [:prefix, :user_id, :company_id, :company_schema, :sharded_schema, :accounting_schema, :accounting_prefix]
            end

            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(method, path, params, jsonapi_args)
              check_jsonapi_args(jsonapi_args)

              user_id           = "'#{jsonapi_args[:user_id]}'"
              company_id        = "'#{jsonapi_args[:company_id]}'"
              company_schema    = jsonapi_args[:company_schema].nil? ? 'NULL' : "'#{jsonapi_args[:company_schema]}'"
              sharded_schema    = jsonapi_args[:sharded_schema].nil? ? 'NULL' : "'#{jsonapi_args[:sharded_schema]}'"
              accounting_schema = jsonapi_args[:accounting_schema].nil? ? 'NULL' : "'#{jsonapi_args[:accounting_schema]}'"
              accounting_prefix = jsonapi_args[:accounting_prefix].nil? ? 'NULL' : "'#{jsonapi_args[:accounting_prefix]}'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def is_error?(result) ; result =~ /^\s*{\s*"errors"\s*:/ ; end

            def check_jsonapi_args(jsonapi_args)
              if jsonapi_args.keys.any? && !(jsonapi_args.keys - valid_keys).empty?
                raise SP::Duh::JSONAPI::Exceptions::InvalidJSONAPIKeyError.new(key: (jsonapi_args.keys - valid_keys).join(', '))
              end
            end

        end

      end
    end
  end
end