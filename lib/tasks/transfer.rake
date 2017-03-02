namespace :sp do
  namespace :duh do
    namespace :transfer do

      desc "Backup a sharded company for restoring to a different database"
      task :backup, [ :company_id, :backup_file ] => :environment do |task, arguments|

        if arguments[:company_id].nil?
          raise "Usage: rake sp:duh:transfer:backup[<company_id> [, <backup_file> ]]"
        end

        company_id = arguments[:company_id].to_i
        backer = SP::Duh::Db::Transfer::Backup.new(ActiveRecord::Base.connection.raw_connection)
        backer.execute(company_id, arguments[:backup_file])

      end

      desc "Restore a sharded company backed up from a different database"
      task :restore, [ :company_id, :backup_file ] => :environment do |task, arguments|

        if arguments[:company_id].nil? || arguments[:backup_file].nil?
          raise "Usage: rake sp:duh:transfer:backup[<company_id>, <backup_file>]"
        end

        company_id = arguments[:company_id].to_i
        backup_file = arguments[:backup_file].to_s
        restorer = SP::Duh::Db::Transfer::Restore.new(ActiveRecord::Base.connection.raw_connection)
        restorer.execute(company_id, backup_file)

      end

      desc "Validate and get info from a sharded company backup"
      task :check, [ :company_id, :backup_file ] => :environment do |task, arguments|

        if arguments[:company_id].nil? || arguments[:backup_file].nil?
          raise "Usage: rake sp:duh:transfer:check[<company_id>, <backup_file>]"
        end

        company_id = arguments[:company_id].to_i
        backup_file = arguments[:backup_file].to_s
        restorer = SP::Duh::Db::Transfer::Restore.new(ActiveRecord::Base.connection.raw_connection)
        restorer.execute(company_id, backup_file, true)

      end

    end
  end
end