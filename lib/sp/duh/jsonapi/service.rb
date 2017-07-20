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

        def initialize(pg_connection, url, adapter = SP::Duh::JSONAPI::Adapters::Db)
          @pg_connection = pg_connection
          @url = url
          protocol = :db
          @configuration = Configuration.new(pg_connection, url)
          SP::Duh::JSONAPI::Model::Base.adapter = @adapter = adapter.new(self)
        end

        def setup
          begin
            create_jsonapi_function()
          rescue StandardError => e
            raise Exceptions::ServiceSetupError.new(nil, e)
          end
          configuration.setup()
        end

        def adapter ; @adapter ; end

        private

          def create_jsonapi_function
            connection.exec %Q[

              CREATE OR REPLACE FUNCTION jsonapi (
                IN method               text,
                IN uri                  text,
                IN body                 text,
                IN user_id              text,
                IN company_id           text,
                IN company_schema       text,
                IN sharded_schema       text,
                IN accounting_schema    text,
                IN accounting_prefix    text,
                OUT http_status         integer,
                OUT response            text
              ) RETURNS record AS '$libdir/pg-jsonapi.so', 'jsonapi' LANGUAGE C;

              CREATE OR REPLACE FUNCTION inside_jsonapi (
              ) RETURNS boolean AS '$libdir/pg-jsonapi.so', 'inside_jsonapi' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_user (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_user' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_company (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_company_schema (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_company_schema' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_sharded_schema (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_sharded_schema' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_accounting_schema (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_schema' LANGUAGE C;

              CREATE OR REPLACE FUNCTION get_jsonapi_accounting_prefix (
              ) RETURNS text AS '$libdir/pg-jsonapi.so', 'get_jsonapi_accounting_prefix' LANGUAGE C;

            ]
          end
      end

    end
  end
end
