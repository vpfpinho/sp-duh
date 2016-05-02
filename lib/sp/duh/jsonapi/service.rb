module SP
  module Duh
    module JSONAPI

      VERSION = '1.0'

      class Status
        OK = 200
        ERROR = 500
      end

      class Service

        def self.protocols ; [ :db, :http ] ; end
        def connection ; @pg_connection ; end
        def url ; @url ; end
        def configuration ; @configuration ; end

        def protocol ; @protocol ; end
        def protocol=(value)
          if !value.to_sym.in?(Service.protocols)
            raise Exceptions::ServiceProtocolError.new(protocol: value.to_sym, protocols: Service.protocols.join(', '))
          end
          @protocol = value.to_sym
        end

        def initialize(pg_connection, url)
          @pg_connection = pg_connection
          @url = url
          protocol = :db
          @configuration = Configuration.new(pg_connection, url)
        end

        def setup
          begin
            create_jsonapi_function()
          rescue StandardError => e
            raise Exceptions::ServiceSetupError.new(nil, e)
          end
          configuration.setup()
        end

        private

          def create_jsonapi_function
            connection.exec %Q[
              CREATE OR REPLACE FUNCTION jsonapi (
                a_method text,
                a_uri text,
                a_body text,
                a_schema text DEFAULT '',
                a_prefix text DEFAULT ''
              ) RETURNS text AS '$libdir/pg_jsonapi.so', 'jsonapi' LANGUAGE C STRICT;
            ]
          end
      end

    end
  end
end