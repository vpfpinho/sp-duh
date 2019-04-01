require 'redis'

namespace :sp do
  namespace :duh do

    desc "Copy OAuth configurations to Redis"
    task :oauth, [ :root_path ] => :environment do |task, args|

      root_path = args.to_hash[:root_path] || '.'
      redis_config = HashWithIndifferentAccess[YAML.load_file(File.join(root_path, 'config/redis.yml'))]
      oauth_config = HashWithIndifferentAccess[YAML.load_file(File.join(root_path, 'config/oauth.yml'))]

      redis = Redis.new(:host => redis_config[:casper][:hostname], :port => redis_config[:casper][:port])


      db_oauth_clients = ActiveRecord::Base.connection.execute("
          SELECT 
          coc.client_id,
          coc.client_secret as secret,
          coc.redirect_uri,
          coc.authorization_code_ttl,
          coc.access_token_ttl,
          coc.refresh_token_ttl,
          coc.scope,
          coc.user_id,
          coc.entity_id,
          c.schema AS entity_schema,
          c.sharded_schema,
          true as on_refresh_issue_new_pair
          FROM casper.oauth_clients coc, common.get_company_schemas_from_id(coc.entity_id) c
        ").to_a

      oauth_config[:'oauth-apps'][1]['clients'] = oauth_config[:'oauth-apps'][1]['clients'].push(db_oauth_clients).flatten

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
