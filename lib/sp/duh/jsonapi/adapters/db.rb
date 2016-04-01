module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < Base

        private

          # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
          def do_request(path, schema, prefix, params, method)
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

        end

      end
    end
  end
end