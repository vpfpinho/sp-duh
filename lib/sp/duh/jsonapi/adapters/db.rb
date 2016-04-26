module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < RawDb

          protected

            def get_error_response(path, error) ; HashWithIndifferentAccess.new(error_response(path, error)) ; end

            def do_request(path, schema, prefix, params, method)
              raw_result = do_request_on_the_db(path, schema, prefix, params, method)
              result = HashWithIndifferentAccess.new(JSON.parse(raw_result))
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [
                SP::Duh::JSONAPI::Status::OK,
                result
              ]
            end

          private

            def is_error?(result) ; !result[:errors].blank? ; end

        end

      end
    end
  end
end