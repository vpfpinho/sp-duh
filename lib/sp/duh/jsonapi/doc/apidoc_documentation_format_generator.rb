module SP
  module Duh
    module JSONAPI
      module Doc

        class ApidocDocumentationFormatGenerator

          def generate(resource_parser, version, doc_folder_path)
            @version = version
            # First create an index of all resources (for relationships linking)
            @resources = {}
            resource_parser.each do |resource|
              next if get_resource_data(resource, :scope) != :public
              @resources[get_resource_name(resource)] = get_resource_link(resource)
            end
            _log "Generating Apidoc documentation for version #{version} in folder #{doc_folder_path}", "JSONAPI::Doc::Generator"
            resource_parser.each do |resource|
              next if get_resource_data(resource, :scope) != :public
              generate_documentation(resource, doc_folder_path)
            end
          end

          private

            def generate_documentation(resource, folder_path)
              readonly = (get_resource_data(resource, :readonly) == true)
              _log "   Generating documentation for resource #{resource}", "JSONAPI::Doc::Generator"
              File.open(File.join(folder_path, "#{get_resource_name(resource)}.js"), File::CREAT | File::TRUNC | File::RDWR) do |f|
                wrap_in_comments(f) { append_lines(f, get_post_documentation(resource)) } if !readonly
                wrap_in_comments(f) { append_lines(f, get_get_documentation(resource)) }
                wrap_in_comments(f) { append_lines(f, get_patch_documentation(resource)) } if !readonly
                wrap_in_comments(f) { append_lines(f, get_delete_documentation(resource)) } if !readonly
                wrap_in_comments(f) { append_lines(f, get_get_documentation(resource, false)) }
              end
            end

            def get_resource_link(resource)
              group = get_resource_data(resource, :group).gsub(' ', '_')
              get_action_name = "#{('Get_' + get_resource_name(resource)).camelcase}"
              link = '#api-' + "#{group}-#{get_action_name}"
              link
            end

            def wrap_in_comments(f) ; write_header(f) ; yield ; write_footer(f) ; end

            def get_get_documentation(resource, single = true)
              documentation = get_api_method_header(resource, :get, single) + get_api_method_params(resource, :get, single) + get_attribute_list(resource, single) + get_api_method_success_example(resource, :get, single)
              documentation.compact!
              documentation
            end

            def get_post_documentation(resource)
              documentation = get_api_method_header(resource, :post) + get_api_method_params(resource, :post) + get_attribute_list(resource) + get_api_method_success_example(resource, :post)
              documentation.compact!
              documentation
            end

            def get_patch_documentation(resource)
              documentation = get_api_method_header(resource, :patch) + get_api_method_params(resource, :patch) + get_attribute_list(resource) + get_api_method_success_example(resource, :patch)
              documentation.compact!
              documentation
            end

            def get_delete_documentation(resource)
              documentation = get_api_method_header(resource, :delete) + get_api_method_params(resource, :delete) + get_api_method_success_example(resource, :delete)
              documentation.compact!
              documentation
            end

            def get_api_method_header(resource, method, single = true)
              data = []
              resource_name = get_resource_name(resource)
              resource_description = get_resource_data(resource, :description)
              resource_description = [ resource_name.titlecase ] if resource_description.blank?
              method_title = "#{get_human_method(method).capitalize} #{uncapitalize(resource_description.first)} #{(single ? '' : 'list')}"
              case
                when method.to_sym.in?([ :patch, :delete ]) || (method.to_sym == :get && single)
                  url = "/#{resource_name}/:id"
                else
                  url = "/#{resource_name}"
              end
              data << "@api {#{method}} #{url} #{method_title}"
              data << "@apiVersion #{@version}"
              data << "@apiName #{(method.to_s + '_' + resource_name + (single ? '' : '_list')).camelcase}"
              data << "@apiGroup #{get_resource_data(resource, :group)}"
              data << "@apiDescription #{method_title}"
              resource_description.each_with_index do |d, i|
                next if i == 0
                data << d
              end
              data
            end

            def get_attribute_type_name_and_description(a)
              data = ""
              type = nil
              if !a[:catalog].nil?
                type = get_type(a[:catalog])
                data += "{#{type}} "
              end
              description = (a[:description] || []).join('. ')
              if a[:association]
                if type && @resources[type.gsub('[]','')]
                  description = '<a href="' + @resources[type.gsub('[]','')] + '">' + description + '</a>'
                end
              end
              data += "#{a[:name]} #{description}"
              data
            end

            def get_api_method_params(resource, method, single = true)
              a = get_resource_data(resource, :id)
              id_param = (a.nil? ? -1 : a.to_i)
              params = []
              case
                when method.to_sym.in?([ :post, :patch, :delete ]) || (method.to_sym == :get && single)
                  if resource[:attributes]
                    resource[:attributes].each_with_index do |a, i|
                      next if a[:readonly] == true
                      next if i == id_param && method.to_sym == :post
                      data = "@apiParam "
                      params << "@apiParam " + get_attribute_type_name_and_description(a)
                      break if i == id_param && method.to_sym.in?([ :get, :delete ])
                    end
                  end
              end
              params = params + get_api_method_param_example(resource, method) if method.to_sym.in?([ :post, :patch ])
              params
            end

            def get_attribute_list(resource, single = true)
              if resource[:attributes]
                resource[:attributes].map do |a|
                  "@apiSuccess " + get_attribute_type_name_and_description(a)
                end
              else
                []
              end
            end

            def get_api_method_param_example(resource, method)
              data = [ "@apiParamExample {json} Request body example", "" ]
              data = data + get_api_method_json(resource, method)
              data
            end

            def get_api_method_success_example(resource, method, single = true)
              data = [ "@apiSuccessExample {json} Success response example", "HTTP/1.1 200 OK", "" ]
              case
                when method.to_sym.in?([ :get, :patch, :post ])
                  data = data + get_api_method_json(resource, :get, single)
              end
              data
            end

            def uncapitalize(text)
              words = text.split(' ')
              words[0] = words[0].downcase
              words.join(' ')
            end

            def get_human_method(m) ; m.to_sym == :post ? :create : (m.to_sym == :patch ? :update : m.to_sym) ; end
            def get_resource_name(r) ; get_resource_data(r, :name) ; end
            def get_resource_data(r, name) ; r[:resource][name] ; end
            def get_attribute(r, i) ; r[:attributes][i] if r[:attributes] ; end
            def get_attribute_data(r, i, name) ; get_attribute(r, i)[name] ; end
            def get_type(r) ; r['format_type'].gsub(/character varying\(\d+\)/, 'text').gsub(' without time zone', '').gsub(' with time zone', '') ; end

            def get_api_method_json(resource, method, single = true)
              json = [ '{' ]
              case
                when method.to_sym.in?([ :post, :patch ]) || (method.to_sym == :get && single)
                  json << '  "data": {'
                  json = json + get_resource_json(resource, method.to_sym != :post, !method.to_sym.in?([ :post, :patch ]))
                  json << '  }'
                when method.to_sym == :get && !single
                  json << '  "data": [{'
                  json = json + get_resource_json(resource)
                  json << '  }]'
              end
              json << '}'
              json
            end

            def get_resource_json(resource, include_id = true, include_readonly = true)
              json = []
              json << '    "type": "' + get_resource_name(resource) + '",'
              id_index = get_resource_data(resource, :id).to_i
              json << '    "id": "' + get_default_value_for_attribute(resource, id_index) + '",' if include_id
              json << '    "attributes": {'
              resource[:attributes].each_with_index do |a, i|
                next if i == id_index
                next if a[:readonly] == true && !include_readonly
                next if a[:association]
                json << '      "' + a[:name].to_s + '": ' + get_default_value_for_attribute(resource, i) + ','
              end
              delete_comma(json)
              json << '    }'
              has_associations = false
              resource[:attributes].each_with_index do |a, i|
                next if !a[:association]
                next if a[:readonly] == true && !include_readonly
                if !has_associations
                  add_comma(json)
                  json << '    "relationships": {'
                  has_associations = true
                end
                json = json + get_association_json(resource, i, a)
              end
              if has_associations
                delete_comma(json)
                json << '    }'
              end
              json
            end

            def add_comma(lines) ; lines[lines.length-1] = lines[lines.length-1] + ',' ; end
            def delete_comma(lines) ; lines[lines.length-1] = lines[lines.length-1][0..lines[lines.length-1].length-2] ; end

            def get_association_json(resource, i, attribute)
              json = []
              if attribute[:association] == :'to-many'
                json << '      "data": ['
                json << '        {'
                json << '          "type": "' + get_type(attribute[:catalog]).sub('[]', '') + '",'
                json << '          "id": ' + get_default_value_for_attribute(resource, i)
                json << '        },'
                json << '        {'
                json << '          "type": "' + get_type(attribute[:catalog]).sub('[]', '') + '",'
                json << '          "id": ' + (get_default_value_for_attribute(resource, i).to_i + 1).to_s
                json << '        }'
                json << '      ],'
              else
                json << '      "data": {'
                json << '        "type": "' + get_type(attribute[:catalog]) + '",'
                json << '        "id": ' + get_default_value_for_attribute(resource, i)
                json << '      },'
              end
              json
            end

            def get_default_value_for_attribute(r, i)
              a = get_attribute(r, i)
              if a.nil?
                'null'
              else
                if a[:name].to_sym == :id || a[:association]
                  return "1"
                else
                  default = a[:example]
                  if !default.nil? && !a[:catalog].nil?
                    default = default.gsub('"','').gsub("'", '')
                    type = get_type(a[:catalog])
                    case type
                    when 'boolean'
                      default = default
                    when 'integer', 'bigint'
                      default = default
                    when 'numeric', 'decimal', 'float'
                      default = default
                    else
                      default = '"' + default + '"'
                    end
                  end
                  if default.nil?
                    default = 'null'
                  end
                  default
                end
              end
            end

            def write_header(f) ; f.puts '/**' ; end
            def append_lines(f, lines) ; lines.each { |l| append_line(f, l) } ; end
            def append_line(f, line = nil) ; f.puts ' * ' + line.to_s ; end
            def write_footer(f) ; f.puts '*/' ; end

        end

      end
    end
  end
end