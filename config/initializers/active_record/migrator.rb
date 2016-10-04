module ActiveRecord
  class Migrator
    cattr_accessor :current_migration_for_transaction_test

    ActiveRecord::Base.connection.instance_eval do
      alias :old_supports_ddl_transactions? :supports_ddl_transactions?

      def supports_ddl_transactions?
        begin
          require File.expand_path(ActiveRecord::Migrator.current_migration_for_transaction_test.filename)

          if MigrationWithoutTransaction.in?(ActiveRecord::Migrator.current_migration_for_transaction_test.name.constantize.ancestors)
            false
          else
            old_supports_ddl_transactions?
          end
        rescue
          old_supports_ddl_transactions?
        end
      end
    end

    alias_method :old_migrate, :migrate

    def migrate(&block)
      ActiveRecord::Base.connection.raw_connection.type_map_for_results = PG::TypeMapAllStrings.new
      ActiveRecord::Base.connection.raw_connection.type_map_for_queries = PG::TypeMapAllStrings.new

      old_migrate do |migration|
        (block.nil? ? true : block.call) && !(ActiveRecord::Migrator.current_migration_for_transaction_test = migration).nil?
      end
    end
  end
end