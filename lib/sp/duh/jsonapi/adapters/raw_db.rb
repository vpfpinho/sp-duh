module SP
  module Duh
    module JSONAPI
      module Adapters

        class RawDb < Base

          protected

            def unwrap_response(response)
              status = response[0]
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
            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(method, path, params, jsonapi_args)
              request_sql = ActiveRecord::Base.send(:sanitize_sql, [
                ":user_id, :company_id, :company_schema, :sharded_schema, :accounting_schema, :accounting_prefix",
                user_id: jsonapi_args[:user_id],
                company_id: jsonapi_args[:company_id],
                company_schema: jsonapi_args[:company_schema],
                sharded_schema: jsonapi_args[:sharded_schema],
                accounting_schema: jsonapi_args[:accounting_schema],
                accounting_prefix: jsonapi_args[:accounting_prefix]
              ], '')

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{request_sql}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{request_sql}) ]
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