module SP
  module Duh
    module JSONAPI
      module Adapters

        class Db < Base

        private

          # Implement the JSONAPI request by direct querying of the JSONAPI function in the database
          def do_request(path, schema, prefix, params, method)
            response = service.connection.exec %Q[
              SELECT * FROM jsonapi('#{method}', '#{url(path)}', '#{escaped_params(params)}', '#{schema}', '#{prefix}');
            ]
            response.first['jsonapi'] if response.first
          end

        end

      end
    end
  end
end