module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < RawDb

          protected

            def get_error_response(path, error) ; HashWithIndifferentAccess.new(error_response(path, error)) ; end

            def do_request(method, path, params)
              process_result(do_request_on_the_db(method, path, params))
            end

            def explicit_do_request(exp_accounting_schema, exp_accounting_prefix, method, path, params)
              process_result(explicit_do_request_on_the_db(exp_accounting_schema, exp_accounting_prefix, method, path, params))
            end

          private

            def is_error?(result) ; !result[:errors].blank? ; end

            def process_result(result)
              result = HashWithIndifferentAccess.new(result)
              result[:response] = JSON.parse(result[:response])
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result[:response]) if is_error?(result[:response])
              [ result[:http_status], result[:response] ]
            end

        end

      end
    end
  end
end