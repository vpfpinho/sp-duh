module SP
  module Duh

    class Repl

      @@help = Array.new
      @@cmds = Array.new

      def initialize (a_pg_conn)
        @pg_conn   = a_pg_conn
        @hist_file = ".#{@prompt.split('>')[0].strip}_history"
        begin
          File.open(@hist_file).each do |line|
            Readline::HISTORY << line.strip
          end
        rescue
          # no history file
        end
        self
      end

      def self.desc (a_msg)
        @@help << a_msg
        @@cmds << a_msg.split(' ')[0].strip
      end

      #
      # http://bogojoker.com/readline/
      # Smarter Readline to prevent empty and dups
      #   1. Read a line and append to history
      #   2. Quick Break on nil
      #   3. Remove from history if empty or dup
      #
      def readline_with_hist_management ()
        line = Readline.readline(@prompt, true)
        return nil if line.nil?
        if line =~ /^\s*$/ or Readline::HISTORY.to_a[-2] == line
          Readline::HISTORY.pop
        end
        line
      end

      def check_db ()
        raise "can't do that! database connection is not valid" if @pg_conn.nil?
      end

      desc 'help                  -- you are looking at it'
      def help ()
        puts @@help
      end

      desc 'quit                  -- exit this shell'
      def quit ()
        puts "quiting"
        File.write(@hist_file, Readline::HISTORY.to_a.join("\n")[0..100])
        exit
      end

      desc 'psql                  -- open sql console'
      def psql ()
        system("psql --user=#{@pg_conn.user} --host=#{@pg_conn.host} #{@pg_conn.db}")
      end

      desc 'open <file>           -- open file with system command'
      def open (a_file)
        system("open #{File.expand_path(a_file)}")
      end

      desc 'debug                 -- enter IRB'
      def debug
         byebug
      end

      def repl ()
        cmdset = @@cmds.abbrev
        while buf = readline_with_hist_management
          begin
            buf.strip!
            args = buf.split(' ')
            command = args[0]
            args    = args[1..-1]
            unless cmdset.has_key?(command)
              puts "#{command} is not a valid command"
              next
            end
            command = cmdset[command]
            if respond_to?(command)
              arity = method(command).arity
              if arity >= 0
                if arity != args.length
                  puts "command #{command} requires #{arity} arguments (#{args.length} given)"
                  next
                end
              elsif arity < 0
                min_args = -arity - 1
                if args.length < min_args or args.length > -arity
                  puts "command #{command} takes #{min_args} to #{-arity} arguments (#{args.length} given)"
                  next
                end
              end
              send(command, *args)
            else
              puts "#{command} is not a valid command"
            end
          rescue SystemExit
            File.write(@hist_file, Readline::HISTORY.to_a.join("\n")[0..100])
            exit
          rescue Exception => e
            puts e.message
            puts e.backtrace
          end
        end

      end
    end

  end
end