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
          sharder = "SP::Duh::Sharding::#{sharder_type.to_s.camelize}".constantize.new(self, shards_table, shard_id_field, shard_value_field)
          add_sharder(sharder)
        end

        def get_table(shard_ids, table_name)
          name = table_name
          ids = shard_ids.reverse
          sharders.reverse.each_with_index do |sharder, i|
            name = sharder.get_table(ids[i], name)
          end
          name
        end

      end

    end
  end
end