module SP
  module Duh
    module Sharding

      class ShardedNamespace

        attr_reader :namespace

        def connection ; @pg_connection ; end
        def sharders ; @sharders ; end

        def initialize(pg_connection, namespace)
          @pg_connection = pg_connection
          @namespace = namespace.to_sym
          @sharders = []
        end

        def add_sharder(sharder)
          sharder.previous = sharders.last
          @sharders << sharder
        end

        def add_new_sharder(sharder_type, shards_table, shard_id_field, shard_value_field)
          begin
            sharder_class = "SP::Duh::Sharding::#{sharder_type.to_s.camelize}".constantize
          rescue NameError => e
            raise Exceptions::InvalidSharderTypeError.new(namespace: namespace.to_s, sharder_type: sharder_type.to_s.camelize)
          end
          sharder = sharder_class.new(self, shards_table, shard_id_field, shard_value_field)
          add_sharder(sharder)
        end

        def get_sharded_table(shard_ids, table_name) ; sharders.any? ? sharders.last.get_sharded_table(shard_ids, table_name) : table_name ; end
      end

    end
  end
end