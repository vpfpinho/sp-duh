module SP
  module Duh
    module JSONAPI
      module Adapters

        class RawDb < Base

          protected

            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(path, schema, prefix, params, method)
              if method == 'GET'
                response = service.connection.exec %Q[
                  SELECT * FROM jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', '#{schema}', '#{prefix}');
                ]
              else
                response = service.connection.exec %Q[
                  SELECT * FROM jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', '#{schema}', '#{prefix}');
                ]
              end
              response.first['jsonapi'] if response.first
            end

            def unwrap_response(response)
              status = response[0]
              result = response[1]
              if status != SP::Duh::JSONAPI::Status::OK
                errors = get_result_errors(result)
                raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(status, "#{errors.first[:detail]}")
              end
              result
            end

            def get_error_response(path, error) ; error_response(path, error).to_json ; end

            def do_request(path, schema, prefix, params, method)
              result = do_request_on_the_db(path, schema, prefix, params, method)
              [
                if result.start_with?('{"errors":')
                  get_result_errors(result).map { |error| error[:status].to_i }.max
                else
                  SP::Duh::JSONAPI::Status::OK
                end,
                result
              ]
            end

          private

            def get_result_errors(result) ; HashWithIndifferentAccess.new(JSON.parse(result))[:errors] ; end
        end

      end
    end
  end
end