#
# Copyright (c) 2011-2017 Cloudware S.A. All rights reserved.
#
# This file is part of sp-duh.
#
# sp-duh is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sp-duh is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with sp-duh.  If not, see <http://www.gnu.org/licenses/>.
#
# encoding: utf-8
#
module SP
  module Duh

    class Repl

      @@help = Array.new
      @@cmds = Array.new

      def initialize (a_pg_conn)
        @see_calc  = false
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

      def save_history
        File.open(@hist_file, "w") do |f|
          Readline::HISTORY.to_a[0..100].each do |line|
            f.write "#{line}\n"
          end
        end
      end

      desc 'quit                  -- exit this shell'
      def quit ()
        puts "quiting"
        save_history()
        exit
      end

      desc 'initsee               -- install pg-see in the database'
      def initsee (a_recreate = false)
        SP::Duh.initsee(@pg_conn, a_recreate)
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

      desc 'pid                   -- get pg backend pid'
      def pid
        pid = @pg_conn.exec("SELECT pg_backend_pid() AS pid")[0]["pid"]
        puts "Backend PID: #{pid}"
      end

      #desc 'reload_jsonapi        -- configure JSON API'
      # @TODO check helpers to reload json and modules with JSM and TD
      #def reload_jsonapi
      #  JSONAPI.service.setup
      #end

      def repl ()
        cmdset = @@cmds.abbrev
        while buf = readline_with_hist_management
          begin
            buf.strip!
            args = buf.split(' ')
            command = args[0]
            args    = args[1..-1]
            unless cmdset.has_key?(command)
              fallback_command(buf)
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
              fallback_command(buf)
            end
          rescue SystemExit
            save_history()
            exit
          rescue Exception => e
            puts e.message
            puts e.backtrace
          end
        end

      end

      def fallback_command (a_command)
        begin
          if @see_calc and @pg_conn != nil
            cmd  = a_command.gsub("'", "''")
            calc = @pg_conn.exec(%Q[
                SELECT json::json FROM see_evaluate_expression('#{cmd}');
              ])
            if calc.cmd_tuples != 1
              puts "unknown error unable to calculate expression"
            else
              jresult = JSON.parse(calc[0]['json'])
              if jresult['error'] != nil
                if jresult['error']['type'] == 'osal::exception'
                  puts jresult['error']['trace']['why']
                else
                  puts jresult['error']
                end
              else
                puts jresult['result']
              end
            end
          else
            puts "#{a_command} is not a valid command"
          end
        rescue => e
          puts e.message
        end
      end

    end
  end
end