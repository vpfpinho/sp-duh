require 'sp/duh/jsonapi/doc/schema_catalog_helper'

module SP
  module Duh
    module JSONAPI
      module Doc

        class VictorPinusMetadataFormatParser
          include Enumerable

          def resources ; @resources || [] ; end
          def resource_names ; resources.map { |r| r.keys.first } ; end

          def initialize(pg_connection)
            @pg_connection = pg_connection
            @schema_helper = SchemaCatalogHelper.new(pg_connection)
          end

          def parse(publisher)
            begin
              publisher = publisher.constantize if publisher.is_a?(String)
              raise Exceptions::InvalidResourcePublisherError.new(publisher: publisher.name) if !publisher.include?(ResourcePublisher)
              @publisher = publisher
            rescue StandardError => e
              raise Exceptions::InvalidResourcePublisherError.new(publisher: publisher.is_a?(String) ? publisher : publisher.name)
            end
            @resources = []
            add_resources_from_folder(publisher.jsonapi_resources_root)
            @resources
          end

          def each(&block)
            @resources.each do |resource|
              block.call(parse_resource(resource))
            end
          end

          private

            def add_resources_from_folder(folder_name)
              @resources ||= []
              # First load resources at the root folder
              Dir.glob(File.join(folder_name, '*.yml')) do |configuration_file|
                add_resources_from_file(configuration_file)
              end
              # Then load resources at the inner folders
              Dir.glob(File.join(folder_name, '*', '*.yml')) do |configuration_file|
                add_resources_from_file(configuration_file)
              end
              @resources
            end

            def add_resources_from_file(configuration_file)
              _log "Loading resources from file #{configuration_file}", "JSONAPI::Doc::Parser"
              configuration =  YAML.load_file(configuration_file)
              if configuration.is_a? Hash
                add_resource(configuration, configuration_file)
              else
                if configuration.is_a? Array
                  configuration.each { |resource| add_resource(resource, configuration_file) }
                else
                  raise Exceptions::InvalidResourceConfigurationError.new(file: configuration_file)
                end
              end
            end

            def add_resource(resource, configuration_file)
              raise Exceptions::InvalidResourceConfigurationError.new(file: configuration_file) if (resource.keys.count != 1)
              resource_name = resource.keys[0]
              _log "   Loading resource #{resource_name}", "JSONAPI::Doc::Parser"
              processed = false
              @resources.each_with_index do |r, i|
                if r.keys.include?(resource_name)
                  @resources[i] = get_resource_index(resource_name, configuration_file)
                  processed = true
                  break
                end
              end
              @resources << get_resource_index(resource_name, configuration_file) if !processed
            end

            def get_resource_index(resource_name, configuration_file)
              {
                resource_name.to_sym => {
                  group: @publisher.name,
                  file: configuration_file
                }
              }
            end

            def parse_resource(resource)
              resource_name = resource.keys[0].to_s
              resource_group = resource.values[0][:group]
              resource_file = resource.values[0][:file]
              _log "   Processing resource #{resource_name} in file #{resource_file}", "JSONAPI::Doc::Parser"
              metadata = parse_file(resource_name, resource_file)
              metadata[:resource] = {} if !metadata.has_key?(:resource)
              metadata[:resource] = metadata[:resource].merge({
                name: resource_name,
                group: resource_group
              })
              metadata
            end

            def parse_file(resource, resource_file)
              metadata = {}
              lines = File.readlines(resource_file)
              table_name = function_name = data_schema = use_schema = nil
              lines.each_with_index do |line, i|

                # Ignore empty lines
                next if line.strip.blank?

                # Process resource definition beginning

                r = get_resource(line)
                # First get the starting line of the resource...
                next if !metadata.has_key?(:resource) && (r.first.nil? || r.first.strip != resource)
                # ... but exit if we reached another resource
                break if metadata.has_key?(:resource) && !r.first.nil? && r.first.strip != resource
                if !r.first.nil? && !metadata.has_key?(:resource)
                  # Get the resource metadata:
                  m = get_metadata_for(lines, i, r)
                  scope = :private
                  if m && (m.first == '[public]' || m.first == '[private]')
                    scope = :public if m.first == '[public]'
                    m.delete_at(0)
                  end
                  metadata[:resource] = {
                    description: m,
                    scope: scope
                  }
                end

                # Process data structure
                table_name = get_value_of('pg-table', line) if table_name.nil?
                function_name = get_value_of('pg-function', line) if function_name.nil?
                data_schema = get_value_of('pg-schema', line) if data_schema.nil?
                use_schema = get_value_of('request-schema', line) if use_schema.nil?

                # Process resource attributes

                if is_beginning_of_attribute_section?(line)
                  metadata[:resource] = {} if !metadata.has_key?(:resource)
                  @schema_helper.clear_settings
                  @schema_helper.add_setting(:schema, data_schema) if !data_schema.nil?
                  @schema_helper.add_setting(:table_name, table_name) if !table_name.nil?
                  @schema_helper.add_setting(:function_name, function_name) if !function_name.nil?
                  @schema_helper.add_setting(:use_schema, use_schema.to_b)
                  metadata[:resource][:catalog] = {
                    sharded_schema: use_schema.to_b
                  }
                  metadata[:resource][:catalog][:schema] = data_schema if !data_schema.nil?
                  metadata[:resource][:catalog][:table_name] = table_name if !table_name.nil?
                  metadata[:resource][:catalog][:function_name] = function_name if !function_name.nil?
                  metadata[:attributes] = []
                end

                a = get_attribute(line)
                if !a.first.nil?
                  metadata[:resource][:id] = metadata[:attributes].length if a.first.strip.to_sym == :id
                  # Get the attribute metadata
                  metadata[:attributes] << {
                    name: a.first.strip,
                    catalog: @schema_helper.get_attribute(a.first),
                    description: get_metadata_for(lines, i, a)
                  }
                end

              end
              _log "   > #{resource} metadata", "JSONAPI::Doc::Parser"
              _log metadata,  "JSONAPI::Doc::Parser"
              metadata
            end

            def get_resource(line)
              resource = /^(?<name>[a-z]+[0-9|a-z|_]*?):\s*(#(?<meta>.*?))*$/.match(line.strip)
              if resource && !resource[:name].strip.to_sym.in?([ :attributes ])
                [ resource[:name], resource[:meta] ]
              else
                [ nil, nil ]
              end
            end
            def is_beginning_of_attribute_section?(line)
              resource = /^(?<name>[a-z]+[0-9|a-z|_]*?):\s*(#(?<meta>.*?))*$/.match(line.strip)
              if resource && resource[:name].strip.to_sym.in?([ :attributes ])
                true
              else
                false
              end
            end
            def get_attribute(line)
              attribute = /^#*\s*-\s*(?<name>[a-z]+[0-9|a-z|_]*?)(#(?<meta>.*?))*$/.match(line.strip)
              if attribute
                [ attribute[:name], attribute[:meta] ]
              else
                [ nil, nil ]
              end
            end
            def get_metadata(line)
              attribute = get_attribute(line)
              metadata = /^# (?<meta>.+?)(\s*\((ex|Ex|default|Default):\s*(?<eg>.+?)\)\s*)*$/.match(line.strip)
              if metadata && attribute.first.nil?
                [ metadata[:meta], metadata[:eg] ]
              else
                [ nil, nil ]
              end
            end
            def get_value_of(attribute, line)
              # First try to match a string value, between double quotes
              data = /^#{attribute}:\s*(\"){1}(?<value>.*?)(\"){1}/.match(line.strip)
              # Then try to match a non-string value
              data = /^#{attribute}:\s*(?<value>.*?)(#(?<meta>.*?))*$/.match(line.strip) if data.nil?
              data ? data[:value] : nil
            end

            def get_metadata_for(enumerable, index, object)
              return if object.first.nil?
              name = object.first.strip
              data = []
              if object.last.nil?
                each_backwards(enumerable, index) do |line|
                  m = get_metadata(line)
                  if m.first
                    data << m.first
                  else
                    break
                  end
                end
              else
                data << object.last.strip
              end
              if data.any?
                data.reverse!
                _log "   > #{name} metadata", "JSONAPI::Doc::Parser"
                _log data,  "JSONAPI::Doc::Parser"
              else
                data = nil
                _log "   > #{name} has no metadata", "JSONAPI::Doc::Parser"
              end
              data
            end

            def each_backwards(enumerable, index, &block)
              while index > 0 do
                 index = index - 1
                 next if enumerable[index].strip.blank?
                 block.call(enumerable[index])
              end
            end

        end

      end
    end
  end
end