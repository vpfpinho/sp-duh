module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter
      def active?
        true
      end

      def initialize_with_redis_connection(connection, logger, connection_parameters, config)
        @redis_connection = Rails.application.redis.optimizations.instance_variable_get(:@data)
        initialize_without_redis_connection(connection, logger, connection_parameters, config)
      end

      private
        def set_standard_conforming_strings
          old, self.client_min_messages = client_min_messages, 'error'
          execute('SET standard_conforming_strings = on', 'SCHEMA') rescue nil
        ensure
          self.client_min_messages = old
        end

        def configure_connection_with_mappings
          # Build Ruby <-> PostgreSQL type mappings
          results_map = PG::BasicTypeMapForResults.new(@connection, @redis_connection)
          queries_map = PG::BasicTypeMapForQueries.new(@connection, @redis_connection)

          # Add a decoder for the NUMERIC PostgreSQL data type
          results_map.add_coder PG::TextDecoder::Float.new(oid: 1700, name: 'float8')
          # Fix: Warning: no type cast defined for type "name" with oid 19. Please cast this type explicitly to TEXT to be safe for future changes.
          results_map.add_coder PG::TextDecoder::String.new(oid: 19, name: 'text')
          # Fix: Warning: no type cast defined for type "regproc" with oid 24. Please cast this type explicitly to TEXT to be safe for future changes.
          results_map.add_coder PG::TextDecoder::String.new(oid: 24, name: 'text')

          # Add decoders for the JSON and JSONB data types (using our custom decoder class)
          results_map.add_coder SP::Duh::Adapters::PG::TextDecoder::Json.new(oid: 114, name: 'json')
          results_map.add_coder SP::Duh::Adapters::PG::TextDecoder::Json.new(oid: 199, name: '_json')
          results_map.add_coder SP::Duh::Adapters::PG::TextDecoder::Json.new(oid: 3802, name: 'jsonb')
          results_map.add_coder SP::Duh::Adapters::PG::TextDecoder::Json.new(oid: 3807, name: '_jsonb')

          # Fix: Remove decoder for Date and Timestamp
          #<PG::TextDecoder::Date:0x0000000857dcf0 "date"  oid=1082>,
          results_map.rm_coder(0, 1082)

          #<PG::TextDecoder::TimestampWithoutTimeZone:0x0000000857db38 "timestamp"  oid=1114>,
          results_map.rm_coder(0, 1114)

          #<PG::TextDecoder::TimestampWithTimeZone:0x0000000857d958 "timestamptz"  oid=1184>,
          results_map.rm_coder(0, 1184)

          #<PG::TextDecoder::Array:0x0000000868cdf8 "_timestamp"  oid=1115>,
          results_map.rm_coder(0, 1115)

          #<PG::TextDecoder::Array:0x0000000868cd30 "_date"  oid=1182>,
          results_map.rm_coder(0, 1182)

          #<PG::TextDecoder::Array:0x0000000868cc40 "_timestamptz"  oid=1185>,
          results_map.rm_coder(0, 1185)

          # <PG::TextDecoder::Bytea:0x007f96afcc1aa8 "bytea"  oid=17>,
          results_map.rm_coder(0, 17)

          # Set PostgreSQL type mappings
          @connection.type_map_for_results = results_map
          @connection.type_map_for_queries = queries_map

          configure_connection_without_mappings
        end

        # Override methods to store catalog data in Redis
        %w(column_definitions primary_key table_exists?).each do |method|
          method_name, punctuation = method.to_s.sub(/([?!=])$/, ''), $1

          class_eval <<-METHOD, __FILE__, __LINE__ + 1
            def #{method_name}_with_redis_cache#{punctuation}(table_name)
              if !!@redis_connection
                @redis_connection.get("postgresql:#{method_name}:\#{table_name}") || begin
                  @redis_connection.set("postgresql:#{method_name}:\#{table_name}", d = #{method_name}_without_redis_cache#{punctuation}(table_name)) ; d
                end
              else
                #{method_name}_without_redis_cache#{punctuation}(table_name)
              end
            end

            alias_method_chain :#{method}, :redis_cache
          METHOD
        end

        def disable_referential_integrity_with_foreign_keys(&block)
          if Rails.env.test?
            transaction do
              begin
                execute "SET CONSTRAINTS ALL DEFERRED"
                yield
              ensure
                execute "SET CONSTRAINTS ALL IMMEDIATE"
              end
            end
          else
            disable_referential_integrity_without_foreign_keys &block
          end
        end

        alias_method_chain :configure_connection, :mappings
        alias_method_chain :disable_referential_integrity, :foreign_keys
        alias_method_chain :initialize, :redis_connection
    end
  end
end

# This module MUST be loaded before the #configure_connection_with_mappings method is called
module PG
  class BasicTypeMapForResults
    def initialize_with_redis_connection(connection, redis_connection = nil)
      @redis_connection = redis_connection
      initialize_without_redis_connection connection
    end

    alias_method_chain :initialize, :redis_connection
  end

  class BasicTypeMapForQueries
    def initialize_with_redis_connection(connection, redis_connection = nil)
      @redis_connection = redis_connection
      initialize_without_redis_connection connection
    end

    alias_method_chain :initialize, :redis_connection
  end

  module BasicTypeRegistry

    private

      def build_coder_maps(connection)
        if !!@redis_connection
          @redis_connection.get('postgresql:coder-maps') || begin
            @redis_connection.set('postgresql:coder-maps', m = get_coder_maps_from_database(connection)) ; m
          end
        else
          get_coder_maps_from_database(connection)
        end
      end

      def get_coder_maps_from_database(connection)
        result = connection.exec(if supports_ranges?(connection)
          <<-SQL
            SELECT n.nspname, t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype
            FROM pg_type as t
              LEFT JOIN pg_range as r ON oid = rngtypid
              LEFT JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname IN ('pg_catalog', 'public')
          SQL
        else
          <<-SQL
            SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput
            FROM pg_type as t
          SQL
        end)

        [
          [0, :encoder, PG::TextEncoder::Array],
          [0, :decoder, PG::TextDecoder::Array],
          [1, :encoder, nil],
          [1, :decoder, nil],
        ].inject([]) do |h, (format, direction, arraycoder)|
          h[format] ||= {}
          h[format][direction] = CoderMap.new result, CODERS_BY_NAME[format][direction], format, arraycoder
          h
        end
      end
  end
end