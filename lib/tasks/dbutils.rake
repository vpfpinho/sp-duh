
desc 'Connect to the Database'
task :pg_connect do
  ENV['RAILS_ENV'] ||= 'development'
  db_config = YAML.load_file(File.join('config', 'database.yml'))[ENV['RAILS_ENV']]
  $db = PG.connect(host: db_config['host'], port: db_config['port'], dbname: db_config['database'], user: db_config['username'], password: db_config['password'])
  $db
end


