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

        reset

        temp_table = "temp_#{@table}"
        perm_table = "#{@schema}.#{@table}"

        vertica.query(<<-SQL)
          CREATE LOCAL TEMPORARY TABLE #{temp_table}
          ON COMMIT DELETE ROWS
          AS SELECT * FROM #{perm_table} LIMIT 0
        SQL

        vertica.copy(<<-SQL) { |handle| handle.write(file.read) }
          COPY #{temp_table} (#{columns.join(",")})
          FROM STDIN DELIMITER E'\t'
          RECORD TERMINATOR E'\n' NULL AS '__NULL__'
          ENFORCELENGTH
          NO COMMIT
        SQL

        if primary_keys.empty?
          vertica.query(<<-SQL)
            INSERT INTO #{perm_table}
              (#{columns.join(",")})
            SELECT
              #{columns.join(",")}
            FROM #{temp_table}
          SQL
        else
          condition = primary_keys.map do |key|
            "#{perm_table}.#{key} = #{temp_table}.#{key}"
          end

          unless empty_table?(perm_table)
            vertica.query(<<-SQL)
              DELETE FROM #{perm_table}
              WHERE EXISTS (
                SELECT 1
                FROM #{temp_table}
                WHERE #{condition.join(" AND ")}
              )
            SQL
          end

          vertica.query(<<-SQL)
            INSERT INTO #{perm_table}
              (#{columns.join(",")})
            SELECT
              #{columns.join(",")}
            FROM (
              SELECT
                #{columns.join(",")},
                row_number() OVER (partition by #{primary_keys.join(",")}) AS r
              FROM #{temp_table}
            ) AS temp
            WHERE r = 1
          SQL
        end

        vertica.query("COMMIT")
      end
    end

    private

    def reset
      @vertica      = nil
      @columns      = nil
      @primary_keys = nil
      @empty        = nil
    end

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
         ORDER BY ordinal_position
       SQL
    end

    def primary_keys
      @primary_keys ||= vertica.query(<<-SQL).map { |column| column[:column_name] }
        SELECT column_name
          FROM v_catalog.primary_keys
         WHERE table_schema = '#{@schema}'
           AND table_name = '#{@table}'
      SQL
    end

    def empty_table?(table)
      @empty ||= vertica.query(<<-SQL).first[:count] == 0
        SELECT count(1) FROM #{table}
      SQL
    end
  end
end
