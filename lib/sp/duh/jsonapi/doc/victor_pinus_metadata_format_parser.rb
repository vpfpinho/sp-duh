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

          def parse(publisher, public_only = true)
            @public_only = public_only
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
              parsed_resource = parse_resource(resource)
              block.call(parsed_resource) if !parsed_resource.nil?
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
              resource_file = resource.values[0][:file]
              _log "   Processing resource #{resource_name} in file #{resource_file}", "JSONAPI::Doc::Parser"
              metadata = parse_file(resource_name, resource_file)
              if !metadata.nil?
                metadata[:resource] = {} if !metadata.has_key?(:resource)
                metadata[:resource] = metadata[:resource].merge({
                  name: resource_name
                })
                if metadata[:resource][:group].blank?
                  # group = /^(?<group>\w+?)_/.match(resource_name)
                  # resource_group = (group ? group[:group].capitalize : resource.values[0][:group])
                  resource_group = resource_name.capitalize
                  metadata[:resource][:group] = resource_group
                end
              else
                _log "   Ignoring PRIVATE resource #{resource_name} in file #{resource_file}", "JSONAPI::Doc::Parser"
              end
              metadata
            end

            def parse_file(resource, resource_file)

              context = nil
              metadata = {}
              indent = nil

              # yaml = YAML.load(resource_file)         # YAML version
              lines = File.readlines(resource_file)   # Full-text version

              table_name = function_name = data_schema = use_schema = nil
              lines.each_with_index do |line, i|

                # Ignore empty lines
                next if line.strip.blank?

                # Process resource definition beginning

                r = get_resource(line, indent)
                # First get the starting line of the resource...
                next if !metadata.has_key?(:resource) && (r.first.nil? || r.first.strip != resource)
                # ... but exit if we reached another resource
                break if metadata.has_key?(:resource) && !r.first.nil? && r.first.strip != resource
                if !r.first.nil? && !metadata.has_key?(:resource)
                  context = :resource
                  indent = r.last.length
                  # Get the resource metadata:
                  m, example, tags = get_metadata_for(lines, i, r)
                  scope = :private
                  scope = :public if 'public'.in?(tags)
                  metadata[:resource] = {
                    description: m,
                    scope: scope
                  }
                  tags.each do |t|
                    case
                    when t.start_with?('group')
                      v = t.split('=')
                      metadata[:resource][:group] = v.last.strip
                    end
                  end
                end

                # Return now if resource is private
                return nil if scope == :private && @public_only

                if context == :resource

                  # Process data structure
                  table_name = get_value_of('pg-table', line) if table_name.nil?
                  function_name = get_value_of('pg-function', line) if function_name.nil?
                  data_schema = get_value_of('pg-schema', line) if data_schema.nil?
                  use_schema = get_value_of('request-schema', line) if use_schema.nil?

                  # Process resource attributes

                  if is_beginning_of_attribute_section?(line)
                    context = :attributes
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
                    next
                  end

                end

                readonly = false

                if context == :attributes

                  readonly = false
                  a = get_attribute(line)
                  if !a.first.nil?
                    metadata[:resource][:id] = metadata[:attributes].length if a.first.strip.to_sym == :id
                    # Get the attribute metadata
                    description, example, tags = get_metadata_for(lines, i, a)
                    readonly = true if 'readonly'.in?(tags)
                    metadata[:attributes] << {
                      name: a.first.strip,
                      catalog: @schema_helper.get_attribute(a.first),
                      description: description,
                      example: example,
                      readonly: readonly
                    }
                  else
                    a = get_jsonapi_attribute(line)
                    if !a.first.nil?
                      case
                      when a.first.in?([ 'to-one', 'to-many' ])
                        context = a.first.to_sym
                        readonly = false
                        next
                      end
                    end
                  end

                end

                if context == :"to-one" || context == :"to-many"
                  a = get_jsonapi_attribute(line)
                  if a.first.nil?
                    type = get_value_of('resource', line)
                    if !type.nil?
                      metadata[:attributes].last.merge!({
                        catalog: {
                          'format_type' => (context == :'to-one' ? type : "#{type}[]")
                        }
                      })
                    end
                  else
                    case
                    when a.first.in?([ 'to-one', 'to-many' ])
                      context = a.first.to_sym
                      next
                    else
                      description, example, tags = get_metadata_for(lines, i, a)
                      readonly = true if 'readonly'.in?(tags)
                      metadata[:attributes] << {
                        name: a.first.strip,
                        association: context,
                        description: description,
                        example: example,
                        readonly: readonly
                      }
                    end
                  end
                end

              end
              _log "   > #{resource} metadata", "JSONAPI::Doc::Parser"
              _log metadata,  "JSONAPI::Doc::Parser"
              metadata
            end

            def get_resource(line, indent = nil)
              if indent.nil?
                resource = /^((?<indent>\s*))?(?<name>[a-z]+[0-9|a-z|_]*?):\s*(#(?<meta>.*?))*$/.match(line)
              else
                resource = /^((?<indent>\s{#{indent}}))?(?<name>[a-z]+[0-9|a-z|_]*?):\s*(#(?<meta>.*?))*$/.match(line)
              end
              if resource
                [ resource[:name], resource[:meta], resource[:indent] ]
              else
                [ nil, nil, nil ]
              end
            end
            def get_jsonapi_attribute(line)
              resource = /^(?<name>[a-z]+[0-9|a-z|_|-]*?):\s*(#(?<meta>.*?))*$/.match(line.strip)
              if resource
                [ resource[:name], resource[:meta] ]
              else
                [ nil, nil ]
              end
            end
            def is_beginning_of_attribute_section?(line)
              line = line.strip
              line = line[1..line.length-1] if line.start_with?('#')
              name, meta = get_jsonapi_attribute(line)
              return true if name && name.to_sym == :attributes
            end
            def get_attribute(line)
              attribute = /^(#\s*)?-\s*(?<name>[a-z]+[0-9|a-z|_]*?)(\s*:\s*)?(#(?<meta>.*?))*$/.match(line.strip)
              if attribute
                [ attribute[:name], attribute[:meta] ]
              else
                [ nil, nil ]
              end
            end
            def get_metadata(line)
              line = line.strip
              attribute = get_attribute(line)
              metadata = line.start_with?('#') ? line[1..line.length-1].strip : nil
              tag = /^#\s*\[(?<tag>(\w|=|\s)+?)\]\s*$/.match(line)
              if tag.nil?
                example = /\((ex|Ex|default|Default):\s*(?<eg>.+?)\)/.match(line)
                if metadata && attribute.first.nil? && metadata != 'attributes:'
                  [ metadata, example ? example[:eg] : nil, nil ]
                else
                  [ nil, nil, nil ]
                end
              else
                [ nil, nil, tag[:tag] ]
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
              example = nil
              tags = []
              if object[1].nil?
                each_backwards(enumerable, index) do |line|
                  m = get_metadata(line)
                  if m.first
                    data << m.first
                    example = m[1] if m[1]
                  else
                    if m[2]
                      tags << m[2]
                    else
                      break
                    end
                  end
                end
              else
                data << object[1].strip
              end
              if data.any?
                data.reverse!
                _log "   > #{name} metadata", "JSONAPI::Doc::Parser"
                _log data,  "JSONAPI::Doc::Parser"
              else
                data = nil
                _log "   > #{name} has no metadata", "JSONAPI::Doc::Parser"
              end
              [ data, example, tags ]
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