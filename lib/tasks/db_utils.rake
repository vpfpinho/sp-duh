
def load_db_from_yml_spec (a_spec)
  a_spec["folders"].each do |elem|
    Pathname.glob("#{MODULE_PATH}/db/#{elem}/**/").each do |folder|
      Dir["#{folder}/*.sql"].each do |file|
        puts "file #{file}"
         $db.exec( File.read(file) )
      end
      # careful!!! we cannot execute automatically some functions on production
      if !a_spec["execute"].nil? && a_spec["execute"].keys.include?(elem)
        can_execute = false
        statements = []

        if a_spec["execute"][elem].is_a?(Array)
          can_execute = true
          statements = a_spec["execute"][elem]
        elsif a_spec["execute"][elem].keys.include? folder.basename.to_s
          can_execute = true
          statements = a_spec["execute"][elem][folder.basename.to_s]
        end

        if can_execute
          statements.each do |to_execute|
            $db.exec( "SELECT * FROM #{to_execute};" )
          end
        end
      end
    end
  end
end

def load_db_config
  ENV['RAILS_ENV'] ||= 'development'
  $db_config = YAML.load_file(File.join('config', 'database.yml'))[ENV['RAILS_ENV']]
end

def connect_to_pg 
  load_db_config()
  $db = PG.connect(host: $db_config['host'], port: $db_config['port'], dbname: $db_config['database'], user: $db_config['username'], password: $db_config['password'])
end

def config_json_api
  $jsonapi_service = SP::Duh::JSONAPI::Service.new($db, JSONAPI_URL)
  $jsonapi_service.setup()
  JSONAPI_PUBLISHERS.each do |publisher|
    $jsonapi_service.configuration.add_publisher(publisher)
  end
  $jsonapi_service.configuration.reload!
end

task :pg_connect do
  $db.close unless $db.nil?
  connect_to_pg()
  $db
end

desc 'Reload jsonapi'
task :config_jsonapi => :pg_connect do
  config_json_api()
end

task :production_safety do
  load_db_config()
  allowed_hosts = %w(localhost tocstaging cloudex 127.0.0.1)
  unless allowed_hosts.include? $db_config['host']
    raise "cannot run tasks for target DB, host: #{$db_config['host']} is not allowed"
  end
end

desc 'Reload GEM functions defined in db_functions.yml'
task :reload_functions => [:production_safety, :pg_connect] do
  load_db_from_yml_spec(YAML.load_file(File.join(MODULE_PATH, 'config', 'db_functions.yml')))
end

desc 'Create a new database seed using db_seed.yml spec'
task :create_db => :production_safety do
  $db.close unless $db.nil?
  %x[dropdb -p #{$db_config['port']} -U #{$db_config['username']} -h #{$db_config['host']} #{$db_config['database']}]
  raise 'dropdb failed, bailing out' unless $?.success?
  %x[createdb -U #{$db_config['username']} #{$db_config['database']}]
  raise 'createdb failed, bailing out' unless $?.success?
  connect_to_pg()
  load_db_from_yml_spec(YAML.load_file(File.join(MODULE_PATH, 'config', 'db_seed.yml')))
  load_db_from_yml_spec(YAML.load_file(File.join(MODULE_PATH, 'config', 'db_functions.yml')))
  config_json_api()
end


