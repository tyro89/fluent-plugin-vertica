module Fluent
  class VerticaOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('vertica', self)

    config_param :host,           :string,  :default => '127.0.0.1'
    config_param :port,           :integer, :default => 5433
    config_param :username,       :string,  :default => 'dbadmin'
    config_param :password,       :string,  :default => nil
    config_param :database,       :string,  :default => nil
    config_param :schema,         :string,  :default => nil
    config_param :table,          :string,  :default => nil
    config_param :ssl,            :bool,    :default => false
    config_param :enforce_length, :bool,    :default => false

    def initialize
      super

      require 'vertica'
      require 'csv'
    end

    def format(tag, time, record)
      values = columns.map { |col| record[col] }
      CSV.generate_line(values, { :col_sep => "\t" })
    end

    def write(chunk)
      chunk.open do |file|
        copy_sql = <<-SQL
            COPY #{@schema}.#{@table} (#{columns.join(",")})
            FROM STDIN DELIMITER E'\t'
          RECORD TERMINATOR E'\n' NULL AS '__NULL__'
          ENFORCELENGTH
           ABORT ON ERROR
         TRICKLE
         #{"ENFORCELENGTH" if @enforce_length}
        SQL

        vertica.copy(copy_sql) do |copy_handle|
          copy_handle.write(file.read)
        end
      end
    end

    private

    def vertica
      @vertica ||= Vertica.connect({
        :host     => @host,
        :user     => @username,
        :password => @password,
        :ssl      => @ssl,
        :port     => @port,
        :database => @database
      })
    end

    def columns
       @columns ||= vertica.query(<<-SQL).map { |column| column[:column_name] }
           SELECT column_name
             FROM columns
            WHERE table_schema ='#{@schema}'
              AND table_name='#{@table}'
         ORDER BY ordinal_position;
       SQL
    end
  end
end
