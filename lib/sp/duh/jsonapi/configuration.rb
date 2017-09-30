module SP
  module Duh
    module JSONAPI

      class Configuration

        CONFIGURATION_TABLE_NAME = 'public.jsonapi_config'
        DEFAULT_SETTINGS_FILE = 'config/jsonapi/settings.yml'

        @@publishers = []

        def settings
          @settings ||= {}
          if @settings.blank?
            load_settings_from_file(File.join(SP::Duh.root, DEFAULT_SETTINGS_FILE))
          end
          @settings
        end

        def settings=(hash)
          @settings = hash
        end

        def resources ; @resources || [] ; end
        def resource_names ; resources.map { |r| r.keys.first } ; end
        def connection ; @pg_connection ; end
        def url ; @url ; end

        def publishers ; @@publishers || [] ; end

        def initialize(pg_connection, url)
          @pg_connection = pg_connection
          @url = url
        end

        def self.add_publisher(publisher)
          begin
            publisher = publisher.constantize if publisher.is_a?(String)
            raise Exceptions::InvalidResourcePublisherError.new(publisher: publisher.name) if !publisher.include?(ResourcePublisher)
            @@publishers << publisher
          rescue StandardError => e
            raise Exceptions::InvalidResourcePublisherError.new(publisher: publisher.is_a?(String) ? publisher : publisher.name)
          end
        end

        def setup
          begin
            create_jsonapi_configuration_store()
          rescue StandardError => e
            raise Exceptions::GenericServiceError.new(e)
          end
        end

        def exists?
          check = connection.exec %Q[ SELECT COUNT(*) FROM #{Configuration::CONFIGURATION_TABLE_NAME} WHERE prefix = '#{url}' ]
          return check.first.values.first.to_i > 0
        end

        def load_from_database
          @resources = []
          @settings = {}
          configuration = connection.exec %Q[ SELECT config FROM #{Configuration::CONFIGURATION_TABLE_NAME} WHERE prefix = '#{url}' ]
          if configuration.first
            configuration = JSON.parse(configuration.first['config'])
            @resources = configuration['resources']
            @settings = configuration.reject { |k,v| k == 'resources' }
          end
          @resources
        end

        def load_from_publishers(replace = false)
          @resources = []
          @settings = {}
          @@publishers.each do |publisher|
             add_resources_from_folder(publisher.jsonapi_resources_root, replace)
          end
          @resources
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

        def reload!
          load_from_publishers(true)
          save
          @resources
        end

        def load_settings_from_file(file_name)
          @settings = YAML.load_file(file_name)
        end

        private

          def create_jsonapi_configuration_store
            connection.exec %Q[
              CREATE TABLE IF NOT EXISTS #{Configuration::CONFIGURATION_TABLE_NAME} (
                prefix varchar(64) PRIMARY KEY,
                config text NOT NULL
              );
            ]
          end

          def definition
            settings.merge(resources: resources)
          end

          def add_resources_from_folder(folder_name, replace)
            @resources ||= []
            # First load resources at the root folder
            Dir.glob(File.join(folder_name, '*.yml')) do |configuration_file|
              add_resources_from_file(configuration_file, replace)
            end
            # Then load resources at the inner folders
            Dir.glob(File.join(folder_name, '*', '*.yml')) do |configuration_file|
              add_resources_from_file(configuration_file, replace)
            end
            @resources
          end

          def add_resources_from_file(configuration_file, replace)
            _log "Processing resources from file #{configuration_file}", "JSONAPI::Configuration"
            configuration =  YAML.load_file(configuration_file)
            if configuration.is_a? Hash
              add_resource(configuration, configuration_file, replace)
            else
              if configuration.is_a? Array
                configuration.each { |resource| add_resource(resource, configuration_file, replace) }
              else
                raise Exceptions::InvalidResourceConfigurationError.new(file: configuration_file)
              end
            end
          end

          def add_resource(resource, configuration_file, replace)
            raise Exceptions::InvalidResourceConfigurationError.new(file: configuration_file) if (resource.keys.count != 1)
            resource_name = resource.keys[0]
            _log "Processing resource #{resource_name}", "JSONAPI::Configuration"
            processed = false
            @resources.each_with_index do |r, i|
              if r.keys.include?(resource_name)
                raise Exceptions::DuplicateResourceError.new(name: resource_name) if !replace
                @resources[i] = resource
                processed = true
                break
              end
            end
            @resources << resource if !processed
          end
      end

    end
  end
end