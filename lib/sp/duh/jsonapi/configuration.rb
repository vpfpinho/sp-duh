module SP
  module Duh
    module JSONAPI

      class Configuration

        CONFIGURATION_TABLE_NAME = 'jsonapi_config'

        def settings ; @settings || {} ; end
        def resources ; @resources || [] ; end
        def resource_names ; resources.map { |r| r.keys.first } ; end
        def connection ; @pg_connection ; end
        def url ; @url ; end

        def initialize(pg_connection, url)
          @pg_connection = pg_connection
          @url = url
        end

        def self.setup(pg_connection)
          begin
            create_jsonapi_configuration_store(pg_connection)
          rescue StandardError => e
            raise Exceptions::GenericServiceError.new(e)
          end
        end

        def exists?
          check = connection.exec %Q[ SELECT COUNT(*) FROM #{Configuration::CONFIGURATION_TABLE_NAME} WHERE prefix = '#{url}' ]
          return check.first.values.first.to_i > 0
        end

        def load
          @resources = []
          @settings = {}
          configuration = connection.exec %Q[ SELECT config FROM #{Configuration::CONFIGURATION_TABLE_NAME} WHERE prefix = '#{url}' ]
          if configuration.first
            configuration = JSON.parse(configuration.first['config'])
            @resources = configuration['resources']
            @settings = configuration.reject { |k,v| k == 'resources' }
            return true
          else
            return false
          end
        end

        def save
          begin
            if exists?
              connection.exec %Q[
                UPDATE #{Configuration::CONFIGURATION_TABLE_NAME} SET config='#{definition.to_json}' WHERE prefix='#{url}';
              ]
            else
              connection.exec %Q[
                INSERT INTO #{Configuration::CONFIGURATION_TABLE_NAME} (prefix, config) VALUES ('#{url}','#{definition.to_json}');
              ]
            end
          rescue StandardError => e
            raise Exceptions::SaveConfigurationError.new(nil, e)
          end
        end

        def add_settings_from_file(file_name)
          @settings = YAML.load_file(file_name)
        end

        def add_resources_from_folder(folder_name)

          @resources ||= []

          Dir.glob(File.join(folder_name, '*','*.yml')) do |configuration_file|
            _log "JSONAPI::Configuration: Processing resources from file #{configuration_file}"
            configuration =  YAML.load_file(configuration_file)
            if configuration.is_a? Hash
              add_resource(configuration, configuration_file)
            else
              if configuration.is_a? Array
                configuration.each { |resource| add_resource(resource, configuration_file) }
              else
                raise Exceptions::InvalidResourceconfigurationError.new(file: configuration_file)
              end
            end
          end

        end

        private

          def self.create_jsonapi_configuration_store(pg_connection)
            pg_connection.exec %Q[
              CREATE TABLE IF NOT EXISTS #{Configuration::CONFIGURATION_TABLE_NAME} (
                prefix varchar(64) PRIMARY KEY,
                config text NOT NULL
              );
            ]
          end

          def definition
            settings.merge(resources: resources)
          end

          def add_resource(resource, configuration_file)
            raise Exceptions::InvalidResourceconfigurationError.new(file: configuration_file) if (resource.keys.count != 1)
            resource_name = resource.keys[0]
            _log "JSONAPI::Configuration: Processing resource #{resource_name}"
            @resources.each { |r| raise Exceptions::DuplicateResourceError.new(name: resource_name) if r.keys.include? resource_name }
            @resources << resource
          end

      end

    end
  end
end