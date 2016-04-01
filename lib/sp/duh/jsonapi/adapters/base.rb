module SP
  module Duh
    module JSONAPI
      module Adapters

        class Base

          def initialize(service)
            @service = service
          end

          def request(path, schema = '', prefix = '', params = nil, method = 'GET')
            begin
              wrap_and_do_request(path, schema, prefix, params, method)
            rescue Exception => e
              [
                SP::Duh::JSONAPI::Status::ERROR,
                get_error_response(path, e)
              ]
            end
          end

          def get(path, schema = '', prefix = '') ; request(path, schema, prefix, nil, 'GET') ; end
          def post(path, schema = '', prefix = '', params = nil) ; request(path, schema, prefix, params, 'POST') ; end
          def patch(path, schema = '', prefix = '', params = nil) ; request(path, schema, prefix, params, 'PATCH') ; end
          def delete(path, schema = '', prefix = '') ; request(path, schema, prefix, nil, 'DELETE') ; end

        protected

          def service ; @service ; end
          def url(path) ; File.join(service.url, path) ; end

          def escaped_params(params)
            params.blank? ?  '' : params.to_json.gsub("'","''")
          end

        private

          def wrap_and_do_request(path, schema, prefix, params, method)
            raw_response = do_request(path, schema, prefix, params, method)
            if raw_response.is_a? Hash
              response = HashWithIndifferentAccess.new(raw_response)
            else
              response = HashWithIndifferentAccess.new(JSON.parse(raw_response))
            end
            [
              if !response[:errors].blank?
                response[:errors].map { |error| error[:status].to_i }.max
              else
                SP::Duh::JSONAPI::Status::OK
              end,
              response
            ]
          end

          # do_request MUST be implemented by each specialized adapter, and returns a JSONAPI string or hash; if string, it will be parsed into a hash
          def do_request(path, schema, prefix, params, method) ; ; end

          def get_error_response(path, error)
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

        end

      end
    end
  end
end