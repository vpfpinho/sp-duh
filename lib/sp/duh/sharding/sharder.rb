module SP
  module Duh
    module Sharding

      class Sharder

        attr_reader :previous

        attr_reader :namespace

        attr_reader :table
        attr_reader :id_field
        attr_reader :value_field

        def initialize(namespace, shards_table, shard_id_field, shard_value_field)
          @namespace = namespace
          @table = shards_table
          @id_field = shard_id_field
          @value_field = shard_value_field
          if @table.blank? || @id_field.blank? || @value_field.blank?
            raise Exceptions::InvalidShardingDefinitionError.new(namespace: @namespace.namespace.to_s, shard_value_field: @value_field.blank? ? '?' : @value_field.to_s, shard_id_field: @id_field.blank? ? '?' : @id_field.to_s, shard_table: @table.blank? ? '?' : @table)
          end
        end

        def get_shards_table(shard_ids = nil)
          get_previous_shard(shard_ids) + table
        end

        def get_shard(shard_ids)
          ids = get_normalized_ids(shard_ids)
          shard_id = ids.slice!(-1)
          shard = nil
          begin
            shards_table = get_shards_table(ids)
            if shard_id.blank?
              raise Exceptions::ShardNotFoundError.new(namespace: namespace.namespace.to_s, shard_table: shards_table, shard_id: shard_id.blank? ? '?' : shard_id.to_s)
            end
            if shard_id.is_a?(String)
              shard = namespace.connection.exec %Q[ SELECT #{value_field} FROM #{shards_table} WHERE #{id_field} = '#{shard_id}' ]
            else
              shard = namespace.connection.exec %Q[ SELECT #{value_field} FROM #{shards_table} WHERE #{id_field} = #{shard_id} ]
            end
          rescue Exception => e
            raise Exceptions::InvalidShardingDefinitionError.new(namespace: namespace.namespace.to_s, shard_value_field: value_field.blank? ? '?' : value_field.to_s, shard_id_field: id_field.blank? ? '?' : id_field.to_s, shard_table: shards_table.blank? ? '?' : shards_table)
          end
          if shard.first
            shard.first.values.first
          else
            raise Exceptions::ShardNotFoundError.new(namespace: namespace.namespace.to_s, shard_table: shards_table, shard_id: shard_id.blank? ? '?' : shard_id.to_s)
          end
        end

        def get_sharded_table(shard_ids, table_name)
          get_fully_qualified_shard(shard_ids).to_s + table_name
        end

        def get_fully_qualified_shard(shard_ids)
          ids = get_normalized_ids(shard_ids)
          shard = get_shard(ids)
          ids.slice!(-1)
          get_previous_shard(ids) + shard
        end

        protected

          attr_writer :previous

        private

          def get_normalized_ids(ids) ; [ids].compact.flatten ; end

          def get_previous_shard(shard_ids)
            if previous
              ids = get_normalized_ids(shard_ids)
              if @_fully_qualified_previous_shard.nil? || ids != @_previous_shard_ids
                @_previous_shard_ids = ids
                @_fully_qualified_previous_shard = previous.get_fully_qualified_shard(@_previous_shard_ids).to_s
              end
            end
            @_fully_qualified_previous_shard.to_s
          end
      end

    end
  end
end