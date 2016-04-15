module SP
  module Duh
    module Migrations
      extend ::ActiveSupport::Concern

      included do
      end

      module ClassMethods

        attr_reader :migrations_root

        def migrator(pg_connection)
          if @migrator.nil? || @migrator.root != migrations_root || @migrator.connection != pg_connection
            @migrator = Migrator.new(pg_connection, migrations_root)
          end
          @migrator
        end

        private

          attr_writer :migrations_root
      end

    end
  end
end