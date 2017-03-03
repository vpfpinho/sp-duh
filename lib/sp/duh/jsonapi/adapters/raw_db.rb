module SP
  module Duh
    module JSONAPI
      module Adapters

        class RawDb < Base

          protected

            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(path, schema, prefix, params, method, sharded_schema = nil)

              response = service.connection.exec ActiveRecord::Base.send(:sanitize_sql, [
                "SELECT * FROM public.jsonapi(:method, :path, :params, :schema, :prefix, :sharded_schema)",
                method: method,
                path: method == 'GET' ? url_with_params_for_query(path, params) : url(path),
                params: method == 'GET' ? '' : params_for_body(params),
                schema: schema,
                prefix: prefix,
                sharded_schema: sharded_schema
              ], '')

              response.first['jsonapi'] if response.first
            end

            def unwrap_response(response)
              status = response[0]
              result = response[1]
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if status != SP::Duh::JSONAPI::Status::OK
              result
            end

            def get_error_response(path, error) ; error_response(path, error).to_json ; end

            def do_request(path, schema, prefix, params, method, sharded_schema = nil)
              result = do_request_on_the_db(path, schema, prefix, params, method, sharded_schema)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [
                SP::Duh::JSONAPI::Status::OK,
                result
              ]
            end

          private

            def is_error?(result) ; result =~ /^\s*{\s*"errors"\s*:/ ; end

        end

      end
    end
  end
end