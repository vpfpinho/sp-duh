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
          redis.hmset(
             "#{service['service_id']}:oauth:#{client['client_id']}"   ,
             "secret"                , "#{client['secret']}",
             "redirect_uri"          , "#{client['redirect_uri']}",
             "authorization_code_ttl", "#{client['authorization_code_ttl']}",
             "access_token_ttl"      , "#{client['access_token_ttl']}",
             "refresh_token_ttl"     , "#{client['refresh_token_ttl']}",
             "scope"                 , "#{client['scope']}"
          )
        end
      end
    end

  end
end