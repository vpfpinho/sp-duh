
desc 'Connect to the Database'
task :pg_connect do
  ENV['RAILS_ENV'] ||= 'development'
  db_config = YAML.load_file(File.join('config', 'database.yml'))[ENV['RAILS_ENV']]
  $db = PG.connect(host: db_config['host'], port: db_config['port'], dbname: db_config['database'], user: db_config['username'], password: db_config['password'])
  $db
end

desc 'Reload jsonapi'
task :config_jsonapi => :pg_connect do
  $jsonapi_service = SP::Duh::JSONAPI::Service.new($db, $jsonapi_url)
  $jsonapi_service.setup()
  $jsonapi_publishers.each do |publisher|
  	$jsonapi_service.configuration.add_publisher(publisher)
  end
  $jsonapi_service.configuration.reload!
end



