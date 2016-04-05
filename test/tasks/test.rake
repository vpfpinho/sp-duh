namespace :test do

  desc "Start the JSONAPI test server"
  task :start do
    system "ruby test/jsonapi/server.rb"
  end

end