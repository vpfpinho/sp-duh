module SP
  module Duh
    module JSONAPI
      module Doc

        class ApidocDocumentationFormatGenerator

          def generate(resource_parser, doc_folder_path)
            _log "Generating Apidoc documentation in folder #{doc_folder_path}", "JSONAPI::Doc::Generator"
            resource_parser.each do |resource|
              next if get_resource_data(resource, :scope) != :public
              ap resource
              generate_documentation(resource, doc_folder_path)
            end
          end

          private

            def generate_documentation(resource, folder_path)
              _log "   Generating documentation for resource #{resource}", "JSONAPI::Doc::Generator"
              File.open(File.join(folder_path, "#{get_resource_name(resource)}.js"), File::CREAT | File::TRUNC | File::RDWR) do |f|
                write_header(f)
                append_lines(f, get_get_documentation(resource))
                write_footer(f)
              end
            end

            def get_get_documentation(resource)
              documentation = get_api_method_header(resource, :get) + get_api_method_params(resource, :get) + get_attribute_list(resource)
              documentation.compact!
              documentation
            end

            def get_api_method_header(resource, method)
              data = []
              resource_name = get_resource_name(resource)
              resource_description = get_resource_data(resource, :description)
              resource_description = [ resource_name.titlecase ] if resource_description.blank?
              case
                when method.to_sym.in?([ :get, :patch, :delete ])
                  url = "/#{resource_name}/:id"
                else
                  url = "/#{resource_name}"
              end
              data << "@api {#{method}} #{url} #{resource_description.first}"
              data << "@apiName #{(method.to_s + '_' + resource_name).camelcase}"
              data << "@apiGroup #{get_resource_data(resource, :group).gsub('::', ' ')}"
              data << "@apiDescription #{method.to_s.capitalize} #{(resource_description.first)}"
              resource_description.each_with_index do |d, i|
                next if i == 0
                data << d
              end
              data
            end

            def get_api_method_params(resource, method)
              case
                when method.to_sym.in?([ :get, :patch, :delete ])
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

            def get_attribute_list(resource)
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

            def get_resource_name(r) ; get_resource_data(r, :name) ; end
            def get_resource_data(r, name) ; r[:resource][name] ; end
            def get_attribute(r, i) ; r[:attributes][i] if r[:attributes] ; end
            def get_attribute_data(r, i, name) ; get_attribute(r, i)[name] ; end
            def get_type(r) ; r['format_type'].gsub(/character varying\(\d+\)/, 'text').gsub(' without time zone', '').gsub(' with time zone', '') ; end

            def write_header(f) ; f.puts '/**' ; end
            def append_lines(f, lines) ; lines.each { |l| append_line(f, l) } ; end
            def append_line(f, line = nil) ; f.puts ' * ' + line.to_s ; end
            def write_footer(f) ; f.puts '*/' ; end

        end

      end
    end
  end
end