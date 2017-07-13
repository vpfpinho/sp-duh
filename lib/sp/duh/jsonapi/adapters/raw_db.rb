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
                ":userId, :companyId, :companySchema, :shardedSchema, :accountingSchema, :accountingPrefix",
                userId: jsonapi_args[:userId],
                companyId: jsonapi_args[:companyId],
                companySchema: jsonapi_args[:companySchema],
                shardedSchema: jsonapi_args[:shardedSchema],
                accountingSchema: jsonapi_args[:accountingSchema],
                accountingPrefix: jsonapi_args[:accountingPrefix]
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