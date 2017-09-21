module SP
  module Duh
    module JSONAPI
      module Adapters

        class Base

          def service ; @service ; end

          def initialize(service)
            @service = service
          end

          def get(path, params, jsonapi_args)
            request('GET', path, params, jsonapi_args)
          end
          def post(path, params, jsonapi_args)
            request('POST', path, params, jsonapi_args)
          end
          def patch(path, params, jsonapi_args)
            request('PATCH', path, params, jsonapi_args)
          end
          def delete(path, jsonapi_args)
            request('DELETE', path, nil, jsonapi_args)
          end

          def get!(path, params, jsonapi_args)
            request!('GET', path, params, jsonapi_args)
          end
          def post!(path, params, jsonapi_args)
            request!('POST', path, params, jsonapi_args)
          end
          def patch!(path, params, jsonapi_args)
            request!('PATCH', path, params, jsonapi_args)
          end
          def delete!(path, jsonapi_args)
            request!('DELETE', path, nil, jsonapi_args)
          end

          alias_method :put, :patch
          alias_method :put!, :patch!

          def unwrap_request
            unwrap_response(yield)
          end

          # do_request MUST be implemented by each specialized adapter, and returns a tuple: the request status and a JSONAPI string or hash with the result
          def do_request(method, path, params, jsonapi_args) ; ; end

          def request(method, path, params, jsonapi_args)
            # As it is now, this method is EXACTLY the same as request!()
            # And it cannot be reverted without affecting lots of changes already made in the app's controllers.
            # TODO: end it, or end the !() version
            # begin
              unwrap_request do
                do_request(method, path, params, jsonapi_args)
              end
            # THIS CAN'T BE DONE, because the same method cannot return both a single result (in case there is NOT an error) and a pair (in case there IS an error)
            # rescue SP::Duh::JSONAPI::Exceptions::GenericModelError => e
            #   [
            #     e.status,
            #     e.result
            #   ]
            # rescue Exception => e
            #   [
            #     SP::Duh::JSONAPI::Status::ERROR,
            #     get_error_response(path, e)
            #   ]
            # end
          end

          def request!(method, path, params, jsonapi_args)
            unwrap_request do
              do_request(method, path, params, jsonapi_args)
            end
          end

          protected

            def url(path) ; File.join(service.url, path) ; end

            def url_with_params_for_query(path, params)
              query = params_for_query(params)
              query.blank? ? url(path) : url(path) + "?" + query
            end

            def params_for_query(params)
              query = ""
              if !params.blank?
                case
                  when params.is_a?(Array)
                    # query = params.join('&')
                    query = params.map{ |v| URI.encode(URI.encode(v).gsub("'","''"), "&") }.join('&')
                  when params.is_a?(Hash)
                    query = params.map do |k,v|
                      if v.is_a?(String)
                        "#{k}=\"#{URI.encode(URI.encode(v).gsub("'","''"), "&")}\""
                      else
                        "#{k}=#{v}"
                      end
                    end.join('&')
                  else
                    query = params.to_s
                end
              end
              query
            end

            def params_for_body(params)
              params.blank? ?  '' : params.to_json.gsub("'","''")
            end

            # unwrap_response SHOULD be implemented by each specialized adapter, and returns the request result as a JSONAPI string or hash and raises an exception if there was an error
            def unwrap_response(response)
              # As the method request() is EXACTLY the same as request!(), and it cannot be reverted without affecting lots of changes already made in the app's controllers...
              # Allow for response being both a [ status, result ] pair (as of old) OR a single result (as of now)
              if response.is_a?(Array)
                status = response[0].to_i
                result = response[1]
                result
              else
                response
              end
            end

            def error_response(path, error)
              {
                errors: [
                  {
                    status: "#{SP::Duh::JSONAPI::Status::ERROR}",
                    code: error.message
                  }
                ],
                links: { self: url(path) },
                jsonapi: { version: SP::Duh::JSONAPI::VERSION }
              }
            end

            # get_error_response MUST be implemented by each specialized adapter, and returns a JSONAPI error result as a string or hash
            def get_error_response(path, error) ; ; end

        end

      end
    end
  end
end