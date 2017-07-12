module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < RawDb

          protected

            def get_error_response(path, error) ; HashWithIndifferentAccess.new(error_response(path, error)) ; end

            def do_request(method, path, params, jsonapi_args)
              result = HashWithIndifferentAccess.new(do_request_on_the_db(method, path, params, jsonapi_args))
              result[:response] = JSON.parse(result[:response])
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result[:response])
              [ result[:http_status], result[:response] ]
            end

          private

            def is_error?(result) ; !result[:errors].blank? ; end

        end

      end
    end
  end
end