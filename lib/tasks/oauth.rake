require 'redis'

namespace :sp do
  namespace :duh do

    desc "Copy OAuth configurations to Redis"
    task :oauth, [ :root_path ] do |task, args|

      root_path = args.to_hash[:root_path] || '.'
      redis_config = HashWithIndifferentAccess[YAML.load_file(File.join(root_path, 'config/redis.yml'))]
      oauth_config = HashWithIndifferentAccess[YAML.load_file(File.join(root_path, 'config/oauth.yml'))]

      redis = Redis.new(:host => redis_config[:master][:hostname], :port => redis_config[:master][:port])
      oauth_config[:'oauth-apps'].each do |service|
        service['clients'].each do |client|
          # First, remove old key
          redis.del "#{service['service_id']}:oauth:#{client['client_id']}"
          client.each do |key, value|
            redis.hmset(
               "#{service['service_id']}:oauth:#{client['client_id']}"   ,
               "#{key}"                 , "#{value}"
            )
          end
        end
      end
    end

  end
end