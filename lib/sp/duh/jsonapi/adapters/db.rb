module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < RawDb

          protected

            def get_error_response(path, error) ; HashWithIndifferentAccess.new(error_response(path, error)) ; end

            def do_request(method, path, params, user_id, company_id, company_schema, sharded_schema, accounting_schema, accounting_prefix)
              raw_result = do_request_on_the_db(method, path, params, user_id, company_id, company_schema, sharded_schema, accounting_schema, accounting_prefix)
              result = HashWithIndifferentAccess.new(raw_result)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [
                result[:http_status],
                HashWithIndifferentAccess.new(JSON.parse(result[:response]))
              ]
            end

          private

            def is_error?(result) ; !result[:errors].blank? ; end

        end

      end
    end
  end
end