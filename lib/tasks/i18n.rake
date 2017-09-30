namespace :i18n do

  desc "Reload all i18n entries into the database"
  task :reload => :environment do

    # Can we use the Rails' connection
    if defined?($db)
      pg_connection = $db
    elsif defined?(Rails)
      pg_connection = ActiveRecord::Base.connection.raw_connection
    else
      raise "No connection to Postgres!"
    end

    # For now, this is the only Excel file we will handle
    excel_filename = File.join(SP::Duh.root, 'config', 'i18n', 'i18n.xlsx')
    loader = SP::Duh::I18n::ExcelLoader.new(excel_filename, pg_connection)
    loader.clear
    loader.reload

  end

end