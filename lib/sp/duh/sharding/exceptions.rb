module SP
  module Duh
    module Sharding
      module Exceptions

        # Sharding errors

        class InvalidSharderTypeError < SP::Duh::Exceptions::GenericDetailedError ; ; end
        class InvalidShardingDefinitionError < SP::Duh::Exceptions::GenericDetailedError ; ; end
        class ShardNotFoundError < SP::Duh::Exceptions::GenericDetailedError ; ; end

      end
    end
  end
end