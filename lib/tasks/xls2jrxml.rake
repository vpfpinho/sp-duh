
desc 'Convert excel model to JRXML'
task :xls2jrxml, [:xls_file] do |task, args|
  args = args.to_hash()
  args[:xls_file] = "#{args[:xls_file]}.xlsx" unless args[:xls_file].end_with?('.xlsx')

  jrxml_base = File.basename(args[:xls_file], '.xlsx')
  Dir.mkdir './tmp' unless Dir.exists?('./tmp')

  converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new(args[:xls_file], nil, true, true, false)
  File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}_compat.jrxml")

  converter = ::Sp::Excel::Loader::Jrxml::ExcelToJrxml.new(args[:xls_file], nil, true, true, true)
  File.rename("#{jrxml_base}.jrxml", "./tmp/#{jrxml_base}.jrxml")
end
