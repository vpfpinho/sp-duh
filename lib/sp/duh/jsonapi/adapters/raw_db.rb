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

            def explicit_do_request(exp_subentity_schema, exp_subentity_prefix, method, path, params)
              process_result(explicit_do_request_on_the_db(exp_subentity_schema, exp_subentity_prefix, method, path, params))
            end

            def specific_service_do_request(method, path, params, service_params)
              process_result(specific_service_do_request_on_the_db(method, path, params, service_params))
            end

          private
            def user_id          ; "'#{service.parameters.user_id}'" ; end
            def entity_id        ; "'#{service.parameters.entity_id}'" ; end
            def entity_schema    ; service.parameters.entity_schema.nil? ? 'NULL' : "'#{service.parameters.entity_schema}'" ; end
            def sharded_schema   ; service.parameters.sharded_schema.nil? ? 'NULL' : "'#{service.parameters.sharded_schema}'" ; end
            def subentity_schema ; service.parameters.subentity_schema.nil? ? 'NULL' : "'#{service.parameters.subentity_schema}'" ; end
            def subentity_prefix ; service.parameters.subentity_prefix.nil? ? 'NULL' : "'#{service.parameters.subentity_prefix}'" ; end

            def process_result(result)
              raise SP::Duh::JSONAPI::Exceptions::GenericModelError.new(result) if is_error?(result)
              [ SP::Duh::JSONAPI::Status::OK, result ]
            end

            # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
            def do_request_on_the_db(method, path, params)
              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{entity_id}, #{entity_schema}, #{sharded_schema}, #{subentity_schema}, #{subentity_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{entity_id}, #{entity_schema}, #{sharded_schema}, #{subentity_schema}, #{subentity_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def explicit_do_request_on_the_db(exp_subentity_schema, exp_subentity_prefix, method, path, params)
              _subentity_schema = "'#{exp_subentity_schema}'"
              _subentity_prefix = "'#{exp_subentity_prefix}'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{user_id}, #{entity_id}, #{entity_schema}, #{sharded_schema}, #{_subentity_schema}, #{_subentity_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{user_id}, #{entity_id}, #{entity_schema}, #{sharded_schema}, #{_subentity_schema}, #{_subentity_prefix}) ]
              end
              response = service.connection.exec jsonapi_query
              response.first if response.first
            end

            def specific_service_do_request_on_the_db(method, path, params, service_params)
              _user_id          = "'"+service_params["user_id"]+"'"
              _entity_id        = "'"+service_params["entity_id"]+"'"
              _entity_schema    = service_params["entity_schema"].blank? ? 'NULL' : "'"+service_params["entity_schema"]+"'"
              _sharded_schema   = service_params["sharded_schema"].blank? ? 'NULL' : "'"+service_params["sharded_schema"]+"'"
              _subentity_schema = service_params["subentity_schema"].blank? ? 'NULL' : "'"+service_params["subentity_schema"]+"'"
              _subentity_prefix = service_params["subentity_prefix"].blank? ? 'NULL' : "'"+service_params["subentity_prefix"]+"'"

              jsonapi_query = if method == 'GET'
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url_with_params_for_query(path, params)}', '', #{_user_id}, #{_entity_id}, #{_entity_schema}, #{_sharded_schema}, #{_subentity_schema}, #{_subentity_prefix}) ]
              else
                %Q[ SELECT * FROM public.jsonapi('#{method}', '#{url(path)}', '#{params_for_body(params)}', #{_user_id}, #{_entity_id}, #{_entity_schema}, #{_sharded_schema}, #{_subentity_schema}, #{_subentity_prefix}) ]
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
