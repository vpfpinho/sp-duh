module SP
  module Duh
    module Migrations

      class Migrator

        attr_reader :root

        def connection ; @pg_connection ; end

        def initialize(pg_connection, migrations_root)
          @pg_connection = pg_connection
          @root = migrations_root
        end

        def up(migration_name) ; migrate(migration_name, :up) ; end
        def down(migration_name) ; migrate(migration_name, :down) ; end

        private

          def migrate(migration_name, direction = :up)
            _log("Migrating #{direction.to_s} #{migration_name}...", "Migrations::Migrator")
            run_all_on(File.join(root, migration_name, direction.to_s))
            _log("[DONE]", "Migrations::Migrator")
          end

          def run_all_on(folder)
            connection.transaction do |t|
              Dir.glob(File.join(folder, '*.sql')).sort.each do |step|
                name = File.basename(step, '.*')
                _log("  #{name}", "Migrations::Migrator")
                t.exec File.read(step)
              end
            end
          end

      end

    end
  end
end