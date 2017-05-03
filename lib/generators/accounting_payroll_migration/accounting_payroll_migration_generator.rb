require 'rails/generators/active_record/migration/migration_generator'

class AccountingPayrollMigrationGenerator < ActiveRecord::Generators::MigrationGenerator
  source_root File.expand_path('../templates', __FILE__)

  # def create_migration_file
  #   set_local_assigns!
  #   migration_template "migration.rb", "db/migrate/#{file_name}.rb"
  # end
end
