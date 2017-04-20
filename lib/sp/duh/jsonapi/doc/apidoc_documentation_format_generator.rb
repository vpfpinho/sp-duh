module SP
  module Duh
    module JSONAPI
      module Doc

        class ApidocDocumentationFormatGenerator

          def generate(resource_parser, version, doc_folder_path)
            @version = version
            _log "Generating Apidoc documentation for version #{version} in folder #{doc_folder_path}", "JSONAPI::Doc::Generator"
            resource_parser.each do |resource|
              next if get_resource_data(resource, :scope) != :public
              generate_documentation(resource, doc_folder_path)
            end
          end

          private

            def generate_documentation(resource, folder_path)
              _log "   Generating documentation for resource #{resource}", "JSONAPI::Doc::Generator"
              File.open(File.join(folder_path, "#{get_resource_name(resource)}.js"), File::CREAT | File::TRUNC | File::RDWR) do |f|
                wrap_in_comments(f) { append_lines(f, get_post_documentation(resource)) }
                wrap_in_comments(f) { append_lines(f, get_get_documentation(resource)) }
                wrap_in_comments(f) { append_lines(f, get_patch_documentation(resource)) }
                wrap_in_comments(f) { append_lines(f, get_delete_documentation(resource)) }
                wrap_in_comments(f) { append_lines(f, get_get_documentation(resource, false)) }
              end
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

            def get_api_method_params(resource, method, single = true)
              case
                when method.to_sym.in?([ :patch, :delete ]) || (method.to_sym == :get && single)
                  a = get_resource_data(resource, :id)
                  if !a.nil?
                    a = get_attribute(resource, a.to_i)
                    if !a.nil?
                      data = "@apiParam "
                      if !a[:catalog].nil?
                        data += "{#{get_type(a[:catalog])}} "
                      end
                      description = a[:description]
                      description = [ a[:name].titlecase ] if description.blank?
                      data += "#{a[:name]} #{a[:description].first}"
                      data = [ data ]
                      description.each_with_index do |d, i|
                        next if i == 0
                        data << d
                      end
                      data
                    else
                      [ nil ]
                    end
                  else
                    [ nil ]
                  end
                else
                  [ nil ]
              end
            end

            def get_attribute_list(resource, single = true)
              if resource[:attributes]
                resource[:attributes].map do |a|
                  data = "@apiSuccess "
                  if !a[:catalog].nil?
                    data += "{#{get_type(a[:catalog])}} "
                  end
                  data += "#{a[:name]} #{(a[:description] || []).join('. ')}"
                  data
                end
              else
                []
              end
            end

            def get_api_method_success_example(resource, method, single = true)
              data = [ "@apiSuccessExample {json} Success response", "HTTP/1.1 200 OK", "" ]
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
                  json = json + get_resource_json(resource, method.to_sym != :post)
                  json << '  }'
                when method.to_sym == :get && !single
                  json << '  "data": [{'
                  json = json + get_resource_json(resource)
                  json << '  }]'
              end
              json << '}'
              json
            end

            def get_resource_json(resource, include_id = true)
              json = []
              json << '    "type": "' + get_resource_name(resource) + '",'
              id_index = get_resource_data(resource, :id).to_i
              json << '    "id": "' + get_default_value_for_attribute(resource, id_index) + '",' if include_id
              json << '    "attributes": {'
              resource[:attributes].each_with_index do |a, i|
                next if i == id_index
                json << '      "' + a[:name].to_s + '": ' + get_default_value_for_attribute(resource, i) + (i == resource[:attributes].length - 1 ? '' : ',')
              end
              json << '    }'
              json
            end

            def get_default_value_for_attribute(r, i)
              a = get_attribute(r, i)
              if a.nil?
                'null'
              else
                if a[:name].to_sym == :id
                  return "1"
                else
                  default = a[:example]
                  if !default.nil? && !a[:catalog].nil?
                    default = default.gsub('"','').gsub("'", '')
                    type = get_type(a[:catalog])
                    case type
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