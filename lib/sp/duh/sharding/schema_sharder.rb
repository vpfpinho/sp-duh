module SP
  module Duh
    module Sharding

      class SchemaSharder < Sharder

        def get_fully_qualified_shard(shard_ids) ; super(shard_ids) + '.' ; end

      end

    end
  end
end