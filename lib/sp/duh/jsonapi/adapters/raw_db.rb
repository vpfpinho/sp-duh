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

            def do_request(method, path, params)
              process_result(do_request_on_the_db(method, path, params))
            end

            def explicit_do_request(exp_accounting_schema, exp_accounting_prefix, method, path, params)
              process_result(explicit_do_request_on_the_db(exp_accounting_schema, exp_accounting_prefix, method, path, params))
            end

            def specific_service_do_request(method, path, params, service_params)
              process_result(specific_service_do_request_on_the_db(method, path, params, service_params))
            end

          private
            def user_id           ; "'#{service.parameters.user_id}'" ; end
            def company_id        ; "'#{service.parameters.company_id}'" ; end
            def company_schema    ; service.parameters.company_schema.nil? ? 'NULL' : "'#{service.parameters.company_schema}'" ; end
            def sharded_schema    ; service.parameters.sharded_schema.nil? ? 'NULL' : "'#{service.parameters.sharded_schema}'" ; end
            def accounting_schema ; service.parameters.accounting_schema.nil? ? 'NULL' : "'#{service.parameters.accounting_schema}'" ; end
            def accounting_prefix ; service.parameters.accounting_prefix.nil? ? 'NULL' : "'#{service.parameters.accounting_prefix}'" ; end

            def process_result(result)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [ SP::Duh::JSONAPI::Status::OK, result ]
            end

            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(method, path, params)
              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{accounting_schema}, #{accounting_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def explicit_do_request_on_the_db(exp_accounting_schema, exp_accounting_prefix, method, path, params)
              _accounting_schema = "'#{exp_accounting_schema}'"
              _accounting_prefix = "'#{exp_accounting_prefix}'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{_accounting_schema}, #{_accounting_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{company_id}, #{company_schema}, #{sharded_schema}, #{_accounting_schema}, #{_accounting_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def specific_service_do_request_on_the_db(method, path, params, service_params)
              _user_id           = "'"+service_params["user_id"]+"'"
              _company_id        = "'"+service_params["company_id"]+"'"
              _company_schema    = service_params["company_schema"].blank? ? 'NULL' : "'"+service_params["company_schema"]+"'"
              _sharded_schema    = service_params["sharded_schema"].blank? ? 'NULL' : "'"+service_params["sharded_schema"]+"'"
              _accounting_schema = service_params["accounting_schema"].blank? ? 'NULL' : "'"+service_params["accounting_schema"]+"'"
              _accounting_prefix = service_params["accounting_prefix"].blank? ? 'NULL' : "'"+service_params["accounting_prefix"]+"'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{_user_id}, #{_company_id}, #{_company_schema}, #{_sharded_schema}, #{_accounting_schema}, #{_accounting_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{_user_id}, #{_company_id}, #{_company_schema}, #{_sharded_schema}, #{_accounting_schema}, #{_accounting_prefix}) ]
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
