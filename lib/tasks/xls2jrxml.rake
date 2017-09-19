
desc 'Convert excel model to JRXML'
task :mock, [:name,:file,:print_server] do |task, args|
  args = args.to_hash()
  args[:data] = File.read(args[:file]) if args.has_key?(:file)
  jrxml_base = File.basename("#{args[:name]}")
  Dir.mkdir './tmp' unless Dir.exists?('./tmp')
  converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new("#{args[:name]}.xlsx", nil, true, true, false)
  File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}_compat.jrxml")
  converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new("#{args[:name]}.xlsx", nil, true, true, true)
  File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}.jrxml")
end