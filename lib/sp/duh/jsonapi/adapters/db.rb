module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < RawDb

          protected

            def unwrap_response(response)
              status = response[0]
              result = response[1]
              if status != SP::Duh::JSONAPI::Status::OK
                errors = result[:errors]
                raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(status, "#{errors.first[:detail]}")
              end
              result
            end

            def get_error_response(path, error) ; HashWithIndifferentAccess.new(error_response(path, error)) ; end

            def do_request(path, schema, prefix, params, method)
              raw_result = do_request_on_the_db(path, schema, prefix, params, method)
              result = HashWithIndifferentAccess.new(JSON.parse(raw_result))
              [
                if !result[:errors].blank?
                  result[:errors].map { |error| error[:status].to_i }.max
                else
                  SP::Duh::JSONAPI::Status::OK
                end,
                result
              ]
            end
        end

      end
    end
  end
end