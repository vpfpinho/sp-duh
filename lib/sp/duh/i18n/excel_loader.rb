require 'sp-excel-loader'

module SP
  module Duh
    module I18n

      class ExcelLoader < ::Sp::Excel::Loader::WorkbookLoader

        def initialize(filename, pg_connection)
          super(filename)
          @connection = pg_connection
        end

        def clear
          @connection.exec "DELETE FROM public.i18n"
        end

        def reload
          export_table_to_pg(@connection,  'public', '', 'i18n')
        end

      end

    end
  end
end
