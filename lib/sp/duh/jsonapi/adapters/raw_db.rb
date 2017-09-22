module SP
  module Duh
    module JSONAPI
      module Adapters

        class RawDb < Base

          protected

            def unwrap_response(response)
              # As the method request() is EXACTLY the same as request!(), and it cannot be reverted without affecting lots of changes already made in the app's controllers...
              # Allow for response being both a [ status, result ] pair (as of old) OR a single result (as of now)
              if response.is_a?(Array)
                status = response[0].to_i
                result = response[1]
                raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if status != SP::Duh::JSONAPI::Status::OK
                result
              else
                # No raise here, we do not know the status...
                response
              end
            end

            def get_error_response(path, error) ; error_response(path, error).to_json ; end

            def do_request(method, path, param)
              result = do_request_on_the_db(method, path, params)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [
                SP::Duh::JSONAPI::Status::OK,
                result
              ]
            end

          private
            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(method, path, params)
              user_id           = "'#{service.parameters.user_id}'"
              company_id        = "'#{service.parameters.company_id}'"
              company_schema    = service.parameters.company_schema.nil? ? 'NULL' : "'#{service.parameters.company_schema}'"
              sharded_schema    = service.parameters.sharded_schema.nil? ? 'NULL' : "'#{service.parameters.sharded_schema}'"
              accounting_schema = service.parameters.accounting_schema.nil? ? 'NULL' : "'#{service.parameters.accounting_schema}'"
              accounting_prefix = service.parameters.accounting_prefix.nil? ? 'NULL' : "'#{service.parameters.accounting_prefix}'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def is_error?(result) ; result =~ /^\s*{\s*"errors"\s*:/ ; end
        end

      end
    end
  end
end