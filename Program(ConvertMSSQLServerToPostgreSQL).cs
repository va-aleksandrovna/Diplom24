using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Diagnostics;

class Program
{
    public static string[] indexes; // массив для индексов и первичных ключей
    public static string[] defaults; // массив для данных полей
    public static string Server = @"LAPTOP-ST0U4SSV\SQLEXPRESS"; // SQL Server
    public static string Server2 = @"127.0.0.1"; // PostgreSQL Server
    public static string Catalog = "AISSNG"; // SQL Server имя БД
    public static string Catalog2; // PostgreSQL имя БД
    public static string Username = string.Empty; // SQL Server имя пользователя
    public static string Username2 = @"postgres"; // PostgreSQL имя пользователя
    public static string Password = string.Empty; // SQL Server пароль
    public static string Password2 = @"123"; // PostgreSQL пароль
    public static string ProviderName2 = @"Npgsql"; // PostgreSQL провайдер
    public static int DatabasePort2 = 5432; // PostgreSQL порт БД
    public static string LastError { get; set; } // сообщение об ошибке

    static int Main(string[] args)
    {
        Stopwatch stopwatch = new Stopwatch();
        long freq = Stopwatch.Frequency; //частота таймера

        Catalog = "AISSNG";
        Catalog2 = Catalog.ToLower(); // в PostgreSQL имя БД всегда в нижнем регистре

        var connectionStringSqlServer = GetConnectionString(Server, Catalog, Username, Password);
        var connectionStringPostgreSql = GetConnectionString(Server2, Catalog2, Username2, Password2, ProviderName2, DatabasePort2);

        // проверка подключения к SQL Server
        if (CheckSqlServer(ref connectionStringSqlServer))
        {
            Console.WriteLine(connectionStringSqlServer);
            Console.WriteLine();
            Console.WriteLine(connectionStringPostgreSql);
            Console.WriteLine();
            Console.WriteLine("-----------------------------");

            stopwatch.Start(); // запуск таймера

            string result = OpenPostgreSql(connectionStringPostgreSql);

            if (string.IsNullOrEmpty(result))
            {
                // копируем БД в PostgreSQL
                Console.WriteLine("-----------------------------");
                result = CopyToPostgreSql(connectionStringSqlServer, connectionStringPostgreSql);
                Console.WriteLine("-----------------------------");
                CreateForeignKeys(connectionStringSqlServer, connectionStringPostgreSql); // получаем внешние ключи для связи таблиц
                Console.WriteLine("-----------------------------");
            }
            stopwatch.Stop();
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine(result + string.Format(" in {0} seconds", (double)stopwatch.ElapsedTicks / freq));
        }

        Console.ReadKey();
        return 0;
    }

    // получаем строку подключения к SQL Server
    public static string GetConnectionString(string server, string catalog, string username, string password)
    {
        string connectionString;

        if (username.Equals(string.Empty))
        {
            connectionString = string.Format(@"Integrated Security=SSPI;Persist Security Info=False;Data Source={0};TrustServerCertificate=true;", server) ;
        }
        else if (username.Contains("\\"))
        {
            connectionString = string.Format(@"Password={0};User ID={1};Integrated Security=SSPI;Persist Security Info=True;Data Source={2}", password, username, server);
        }
        else
        {
            connectionString = string.Format(@"Password={0};Persist Security Info=True;User ID={1};Data Source={2}", password, username, server);
        }

        if (!catalog.Equals(string.Empty))
        {
            connectionString += ";Initial Catalog=" + catalog; // добавляем имя БД, если не пустое
        }

        return connectionString;
    }

    // получаем строку подключения к PostgreSQL с именем провайдера и портом 
    public static string GetConnectionString(string server, string catalog, string username, string password, string providername, int port)
    {
        string connectionString = "";

        if (string.IsNullOrEmpty(providername))
        {
            providername = "System.Data.SqlClient";
        }

        if (providername == @"System.Data.SqlClient")
        {
            connectionString = GetConnectionString(server, catalog, username, password);
        }
        else if (providername == @"Npgsql")
        {
            if (string.IsNullOrEmpty(catalog))
            {
                catalog = @"postgres";
            }

            connectionString = string.Format("Server={0};Port={1};User Id={2};Password={3};Database={4}", server, port, username, password, catalog);
        }
        else
        {
            try
            {
                string errorMessage = @"GetConnectionString() неизвестное имя провайдера: " + providername;
                throw new Exception(errorMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка: {ex.Message}");
            }
        }

        return connectionString;
    }

    // сначала пробуем подключиться к SQL Express, если не получается, то к SQL Server
    public static bool CheckSqlServer(ref string connectionStringSqlServer)
    {
        bool result; // возвращает результат подключения

        using (var sqlConnection = new SqlConnection(connectionStringSqlServer))
        {
            sqlConnection.Open();
            result = sqlConnection.State == System.Data.ConnectionState.Open; // проверка на успешность открытия соединения

            if (result)
            {
                Console.WriteLine("SQL Server Express " + sqlConnection.ServerVersion);
            }
            else
            {
                Server = @"(local)";
                connectionStringSqlServer = GetConnectionString(Server, Catalog, Username, Password);

                using (var sqlConnection2 = new SqlConnection(connectionStringSqlServer))
                {
                    sqlConnection2.Open();
                    result = sqlConnection2.State == System.Data.ConnectionState.Open;

                    if (result)
                    {
                        Console.WriteLine("SQL Server " + sqlConnection.ServerVersion);
                    }
                    else
                    {
                        Console.WriteLine("Не удалось подключиться к SQL Server");
                    }
                }
            }
        }

        Console.WriteLine();
        return result;
    }

    // открываем соединение с PostgreSql или создаем если оно не существует
    // возвращает пустую строку при успеном выполнении
    public static string OpenPostgreSql(string connectionStringPostgresql)
    {
        bool catalogExists = false;

        using (var pgConnection = new NpgsqlConnection(connectionStringPostgresql))
        {
            try
            {
                pgConnection.Open();
                catalogExists = pgConnection.State == ConnectionState.Open;
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);

                if (!ex.Message.Contains("�� ����������"))
                {
                    // все другие ошибки, кроме "не существует"
                    return ex.Message;
                }
            }
        }

        if (catalogExists)
        {
            // ЧТО НУЖНО СДЕЛАТЬ: "Вы уверены, что хотите перезаписать: " + Каталог 2
            //вернуть "Отменено".;
            Console.WriteLine(@"Удаление БД " + Catalog2);

            try
            {
                var connectionStringDelete = GetConnectionString(Server2, string.Empty, Username2, Password2, ProviderName2, DatabasePort2);
                DeleteDatabase(connectionStringDelete, Catalog2);
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

        // создаем новую БД
        string pgSql = string.Format("CREATE DATABASE \"{0}\";", Catalog2);
        Debug.Print(pgSql);

        var connectionWithoutCatalog = GetConnectionString(Server2, string.Empty, Username2, Password2, ProviderName2, DatabasePort2);
        var resultCreate = ExecuteSqlScript(connectionWithoutCatalog, pgSql);

        using (var pgConnection2 = new NpgsqlConnection(connectionStringPostgresql))
        {
            if (resultCreate)
            {
                pgConnection2.Open();
            }

            if (pgConnection2.State != ConnectionState.Open)
            {
                return @"Ошибка, не удалось открыть соединение: " + connectionStringPostgresql;
            }
        }

        // скрипт для использования UUID в качестве первичного ключа с использованием функции uuid_generate_v1()
        ExecuteSqlScript(connectionStringPostgresql, "CREATE EXTENSION \"uuid-ossp\";");

        //ExecuteSqlScript(connectionStringPostgresql, "SET standard_conforming_strings = off;");

        return string.Empty;
    }


    // определяем используется ли PostgreSQL
    public static bool IsPostgresql(string connectionstring)
    {
        return connectionstring.ToLower().Contains("server=");
    }


    // удаление БД с PostgreSQL
    public static void DeleteDatabase(string connectionStringDelete, string catalog)
    {
        NpgsqlConnection.ClearAllPools(); // очищаем все активные пулы соединений Npgsql

        using (var connectiondelete = new NpgsqlConnection(connectionStringDelete))
        {
            connectiondelete.Open();
            using (var command = new NpgsqlCommand(string.Format("DROP DATABASE \"{0}\";", catalog.ToLower()), connectiondelete))
            {
                command.ExecuteNonQuery();
            }
        }
        return;
    }

    // Выполнение скрипта на SQL Server или PostgreSQL
    public static bool ExecuteSqlScript(string connectionString, string sqlCommand)
    {
        bool result = false;
        LastError = string.Empty;

        if (IsPostgresql(connectionString))
        {
            using (var pgConnection = new NpgsqlConnection(connectionString))
            {
                using (var cmd = new NpgsqlCommand(sqlCommand, pgConnection))
                {
                    try
                    {
                        if (pgConnection.State != ConnectionState.Open)
                        {
                            pgConnection.Open();
                        }
                        cmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LastError = ex.Message;
                        Debug.Print("ExecuteSqlScript() ошибка: " + ex.Message);
                        Console.WriteLine("ExecuteSqlScript() ошибка: " + ex.Message);
                    }
                    finally
                    {
                        pgConnection.Close();
                    }
                }
            }
        }
        else
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                using (var cmd = new SqlCommand(sqlCommand, sqlConnection))
                {
                    try
                    {
                        if (sqlConnection.State != ConnectionState.Open)
                        {
                            sqlConnection.Open();
                        }
                        cmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LastError = ex.Message;
                        Debug.Print("ExecuteSqlScript() ошибка: " + ex.Message);
                        Console.WriteLine("ExecuteSqlScript() ошибка: " + ex.Message);
                    }
                    finally
                    {
                        sqlConnection.Close();
                    }
                }
            }
        }
        return result;
    }

    // копируем данные БД
    public static string CopyToPostgreSql(string connectionStringSqlServer, string connectionStringPostgresql)
    {
        var sqlTables = new List<string>();
        var sqlColumns = new List<string>();
        var sqlColumnsB = new List<string>();
        var pgColumns = new List<string>();
        var pgColumns2 = new List<string>();
        int pgTableCount = 0;

        using (var pgConnection = new NpgsqlConnection(connectionStringPostgresql))
        {
            pgConnection.Open();

            // копируем все таблицы из БД SQL Server
            using (var sqlConnection = new SqlConnection(connectionStringSqlServer))
            {
                sqlConnection.Open();

                if (sqlConnection.State == ConnectionState.Open)
                {
                    // возвращается таблица данных о таблицах в БД без представлений
                    DataTable dtTables = sqlConnection.GetSchema("Tables", new string[] { null, null, null, "BASE TABLE" });

                    foreach (DataRow rowTable in dtTables.Rows)
                    {
                        string sqlSchema = (string)rowTable[1];
                        string sqlTablename = (string)rowTable[2];
                        if (sqlTablename != "sysdiagrams") // кроме таблицы пользователе или системной таблицы
                        {
                            string pgTableName = sqlTablename.Replace(' ', '_');

                            Console.WriteLine(sqlTablename);
                            sqlTables.Add(sqlTablename);
                            sqlColumns.Clear();
                            sqlColumnsB.Clear();
                            pgColumns.Clear();
                            pgColumns2.Clear();

                            // получаем столбцы таблицы, изменяем типы данных, получаем первичные ключи
                            string pgCreate = GetData(sqlConnection, sqlTablename, ref sqlColumns, ref sqlSchema);

                            // добавляем схему для таблицы
                            var pgSchema = sqlSchema.Replace(' ', '_');
                            pgTableName = pgSchema + "." + pgTableName;
                            bool result1 = ExecuteSqlScript(connectionStringPostgresql, "create schema if not exists " + pgSchema + ";");

                            // создаем таблицу, примерно так:
                            // CREATE TABLE test (
                            //    id            uuid NOT NULL,
                            //    number        int NULL,
                            //    CONSTRAINT    pk_name PRIMARY KEY (id) )
                            var pgSql1 = string.Format("create table {0} ({1});", pgTableName, pgCreate);
                            Debug.Print(pgSql1);
                            bool result2 = ExecuteSqlScript(connectionStringPostgresql, pgSql1);

                            // задаем ROWGUIDCOL столбцы таблицы
                            if (defaults != null)
                            {
                                foreach (string pgDefault in defaults)
                                {
                                    Debug.Print(pgDefault);
                                    ExecuteSqlScript(connectionStringPostgresql, pgDefault);
                                }
                            }

                            for (int i = 0; i < sqlColumns.Count; i++)
                            {
                                pgColumns.Add(sqlColumns[i].ToLower().Replace(' ', '_'));
                                pgColumns2.Add("@" + sqlColumns[i].ToLower().Replace(' ', '_'));
                                sqlColumnsB.Add("[" + sqlColumns[i] + "]");
                            }

                            string sqlColumnsJoin = string.Join(",", sqlColumnsB);
                            string pgColumnsJoin = string.Join(",", pgColumns);
                            string pgParameters = "@" + string.Join(",@", pgColumns);

                            // читаем значения из таблицы
                            var command = sqlConnection.CreateCommand();
                            var msSql = string.Format(@"SELECT {0} FROM [{1}].[{2}].[{3}]", sqlColumnsJoin, Catalog, sqlSchema, sqlTablename);
                            Debug.Print(msSql);
                            command.CommandText = msSql;

                            // вставляем значения таблицы в новую БД
                            using (var dr = command.ExecuteReader(CommandBehavior.Default))
                            {
                                uint count = 0;
                                // создаем запрос с названиями столбцов и параметрами
                                var pgSql = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", pgTableName, pgColumnsJoin, pgParameters);
                                object[] rowObjects = new object[sqlColumns.Count];
                                while (dr.Read())
                                {
                                    int result = dr.GetValues(rowObjects);

                                    result = ExecuteParameterQuery(pgConnection, pgSql, pgColumns2, rowObjects);
                                    if (result < 1)
                                    {
                                        Debug.Print(@"Ошибка при создании запроса: " + pgSql);
                                    }
                                    count++;
                                }
                                Console.WriteLine("Вставлено " + count + " записей");
                                Console.WriteLine();
                            }
                            pgTableCount++;
                        }
                    }
                }
            }
        }
        return string.Format("Скопировано {0} таблиц из {1}", pgTableCount, sqlTables.Count);
    }

    // получаем таблицы, изменяем типы данных, заполняем индексы
    public static string GetData(SqlConnection sqlConnection, string sqlTablename, ref List<string> sqlColumns, ref string schema)
    {
        // 0 = TABLE_CATALOG; 1 = TABLE_SCHEMA; 2 = TABLE_NAME; 3 = COLUMN_NAME 
        var columns = new string[4];
        columns[2] = sqlTablename;
        DataTable dtColumns = sqlConnection.GetSchema("Columns", columns);
        List<string> pgFields = new List<string>();
        string pgFieldsString;

        foreach (DataRow rowColumn in dtColumns.Rows)
        {
            string field;
            schema = rowColumn["table_schema"].ToString();
            string columnName = rowColumn["column_name"].ToString();
            string dataType = rowColumn["data_type"].ToString();
            string characterMaxLen = rowColumn["character_maximum_length"].ToString();
            string nullable = rowColumn["is_nullable"].ToString();
            sqlColumns.Add(columnName);
            columnName = columnName.Replace(' ', '_');

            if (string.IsNullOrEmpty(characterMaxLen))
            {
                field = columnName + " " + dataType;
            }
            else if (characterMaxLen == "-1") // -1 для данных типа XML и больших значений
            {
                field = columnName + " " + dataType + "(max)";
            }
            else
            {
                field = columnName + " " + dataType + "(" + characterMaxLen + ")";
            }

            if (nullable == "YES")
            {
                field += " NULL";
            }
            else
            {
                field += " NOT NULL";
            }
            pgFields.Add(field);
        }

        // получаем ROWGUIDCOL столбцы таблицы
        defaults = GetDefaults(sqlConnection, sqlTablename, schema).ToArray();

        // получаем первичные ключи
        indexes = GetIndexes(sqlConnection, sqlTablename, schema).ToArray();

        if (indexes != null)
        {
            foreach (string index in indexes)
            {
                if (index.StartsWith("constraint"))
                {
                    Debug.Print("Primary key = " + index);
                    pgFields.Add(index);
                }
            }
        }

        // преобразование типов данных
        pgFieldsString = string.Join(",", pgFields);
        pgFieldsString = pgFieldsString.ToLower();

        //pgFieldsString = pgFieldsString.Replace(" smallint", " smallint");
        //pgFieldsString = pgFieldsString.Replace(" char", " char");
        //pgFieldsString = pgFieldsString.Replace(" varchar", " varchar");
        pgFieldsString = pgFieldsString.Replace(" varchar(max)", " text");
        //pgFieldsString = pgFieldsString.Replace(" date", " date");
        //pgFieldsString = pgFieldsString.Replace(" time", " time");
        //pgFieldsString = pgFieldsString.Replace(" real", " real");
        pgFieldsString = pgFieldsString.Replace(" bit ", " boolean ");
        pgFieldsString = pgFieldsString.Replace(" nvarchar(max)", " varchar");
        pgFieldsString = pgFieldsString.Replace(" smalldatetime", " timestamp(0)");
        pgFieldsString = pgFieldsString.Replace(" tinyint", " smallint");
        pgFieldsString = pgFieldsString.Replace(" filestream", " ");////////////////////////
        pgFieldsString = pgFieldsString.Replace(" uniqueidentifier", " uuid");
        pgFieldsString = pgFieldsString.Replace(" varbinary(max)", " bytea");

        pgFieldsString = pgFieldsString.Replace(" nvarchar", " varchar");
        pgFieldsString = pgFieldsString.Replace(" nchar", " char");
        pgFieldsString = pgFieldsString.Replace(" float", " double precision");
        //pgFieldsString = pgFieldsString.Replace(" double", " double precision");
        pgFieldsString = pgFieldsString.Replace(" binary", " bytea");
        pgFieldsString = pgFieldsString.Replace(" int", " integer");
        // не все типы данных

        return pgFieldsString;
    }

    // получаем столбцы ROWGUIDCOL таблицы в виде: ALTER TABLE name ALTER COLUMN col SET DEFAULT uuid_generate_v1();
    public static List<string> GetDefaults(SqlConnection sqlConnection, string tableName, string schema)
    {
        var defaults = new List<string>();
        string pgTableName = schema + "." + tableName;
        pgTableName = pgTableName.ToLower();
        try
        {
            var command = sqlConnection.CreateCommand();
            // задаем команду для получения столбцов ROWGUIDCOL
            var sql = string.Format(@"SELECT col.name, is_rowguidcol, is_identity
                                            FROM    sys.indexes ind
                                                    INNER JOIN sys.index_columns ic
                                                        ON ind.object_id = ic.object_id
                                                           AND ind.index_id = ic.index_id
                                                    INNER JOIN sys.columns col
                                                        ON ic.object_id = col.object_id
                                                           AND ic.column_id = col.column_id
                                                    INNER JOIN sys.tables t
                                                        ON ind.object_id = t.object_id
                                            WHERE   t.is_ms_shipped = 0 
                                                    AND (col.is_rowguidcol > 0 OR col.is_identity > 0)
                                                    AND OBJECT_SCHEMA_NAME(ind.object_id) = '{0}'
		                                            AND OBJECT_NAME(ind.object_id) = '{1}'", schema, tableName);
            command.CommandText = sql;
            using (var dr = command.ExecuteReader(CommandBehavior.Default)) // запускаем ее и читаем
            {
                while (dr.Read())
                {
                    // 0 = col.name 1 = rowguidcol 2 = identity
                    object[] rowObjects = new object[3];
                    int result = dr.GetValues(rowObjects);
                    var columnName = rowObjects[0].ToString().ToLower();
                    var rowguidcol = rowObjects[1].ToString();
                    var identity = rowObjects[2].ToString();

                    if (!string.IsNullOrEmpty(columnName))
                    {
                        columnName = columnName.Replace(' ', '_');
                        if (rowguidcol == "True")
                        {
                            string df = string.Format("alter table {0} alter column {1} set default uuid_generate_v1();", pgTableName, columnName);
                            defaults.Add(df);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Debug.Print("GetDefaults() " + ex.Message);
        }
        return defaults;
    }

    // получаем первичные ключи
    public static List<string> GetIndexes(SqlConnection sqlConnection, string tableName, string schema)
    {
        string indexName = string.Empty;
        var indexes = new List<string>();
        try
        {
            var command = sqlConnection.CreateCommand();
            var sql = string.Format("EXEC sys.sp_helpindex @objname = N'{0}.{1}' ", schema, tableName);
            command.CommandText = sql;
            using (var dr = command.ExecuteReader(CommandBehavior.Default))
            {
                while (dr.Read())
                {
                    // 0 = index_name 1 = index_description 2 = index_keys.
                    object[] rowObjects = new object[3];
                    int result = dr.GetValues(rowObjects);
                    indexName = rowObjects[0].ToString().ToLower();
                    var description = rowObjects[1].ToString().ToLower();
                    string indexKeys = rowObjects[2].ToString().ToLower();
                    indexName = indexName.Replace(' ', '_');
                    indexKeys = indexKeys.Replace(", ", ",");
                    indexKeys = indexKeys.Replace(' ', '_');
                    if (description.Contains("primary key"))
                    {
                        string pk = string.Format("constraint {0} primary key({1})", indexName, indexKeys);
                        indexes.Add(pk);
                    }
                }
            }
        }
        catch
        {
            Debug.Print("GetIndexes() ошибка");
        }

        if (string.IsNullOrEmpty(indexName))
        {
            // первичные ключи не найдены
            return null;
        }
        return indexes;
    }

    // выполняем запрос с полученными записями (заполняем таблицу)
    public static int ExecuteParameterQuery(NpgsqlConnection connection, string query, List<string> columnList, object[] records)
    {
        int result = 0;
        try
        {
            // добавляем значения на место параметров
            using (var command = new NpgsqlCommand(query, connection))
            {
                if (records != null)
                {
                    for (int i = 0; i < columnList.Count; i++)
                    {
                        //var columnName = columnList[i];
                        //var obj = records[i];
                        if (records[i].GetType() == typeof(string))
                        {
                            var pk = (string)records[i];
                            records[i] = pk.Replace("\0", "");
                        }
                        command.Parameters.AddWithValue(columnList[i], records[i]);
                        //Console.WriteLine(columnName +""+records[i].ToString());
                    }
                }

                //for(int i=0; i < columnList.Count; i++)
                //{
                //    Console.WriteLine(command.Parameters[i].Value);
                //}
                result = command.ExecuteNonQuery(); // выполняем запрос
                command.Parameters.Clear();
            }
        }
        catch (Exception ex)
        {
            LastError = ex.Message;
            Debug.Print(LastError);
            Console.WriteLine(LastError);
        }
        return result;
    }

    // ищем внешние ключи и отправляем запросы
    public static void CreateForeignKeys(string connectionStringSqlServer, string connectionStringPostgresql)
    {
        Console.WriteLine(@"Поиск внешних ключей");
        var foreignKey = GetForeignKeyInformation(connectionStringSqlServer);
        if (foreignKey.Length < 1)
        {
            Console.WriteLine(@"не найдены");
        }
        foreach (string f in foreignKey)
        {
            ExecuteSqlScript(connectionStringPostgresql, f);
        }
    }

    // получаем внешние ключи
    public static string[] GetForeignKeyInformation(string connectionString)
    {
        var pgStatements = new List<string>();

        using (var cn = new SqlConnection(connectionString))
        {
            cn.Open();
            var command = cn.CreateCommand();
            command.CommandText = @"
BEGIN
	DECLARE @tbl_foreign_key_columns TABLE ( constraint_name NVARCHAR(128),
											 base_schema_name NVARCHAR(128),
											 base_table_name NVARCHAR(128),
											 base_column_id INT,
											 base_column_name NVARCHAR(128),
											 unique_schema_name NVARCHAR(128),
											 unique_table_name NVARCHAR(128),
											 unique_column_id INT,
											 unique_column_name NVARCHAR(128),
											 base_object_id INT,
											 unique_object_id INT )
	INSERT  INTO @tbl_foreign_key_columns ( constraint_name, base_schema_name, base_table_name, base_column_id, base_column_name, unique_schema_name, unique_table_name, unique_column_id, unique_column_name, base_object_id, unique_object_id )
			SELECT  FK.name AS constraint_name, S.name AS base_schema_name, T.name AS base_table_name, C.column_id AS base_column_id, C.name AS base_column_name, US.name AS unique_schema_name, UT.name AS unique_table_name, UC.column_id AS unique_column_id, UC.name AS unique_column_name, T.[object_id], UT.[object_id]
			FROM    sys.tables AS T
			INNER JOIN sys.schemas AS S
			ON      T.schema_id = S.schema_id
			INNER JOIN sys.foreign_keys AS FK
			ON      T.object_id = FK.parent_object_id
			INNER JOIN sys.foreign_key_columns AS FKC
			ON      FK.object_id = FKC.constraint_object_id
			INNER JOIN sys.columns AS C
			ON      FKC.parent_object_id = C.object_id
					AND FKC.parent_column_id = C.column_id
			INNER JOIN sys.columns AS UC
			ON      FKC.referenced_object_id = UC.object_id
					AND FKC.referenced_column_id = UC.column_id
			INNER JOIN sys.tables AS UT
			ON      FKC.referenced_object_id = UT.object_id
			INNER JOIN sys.schemas AS US
			ON      UT.schema_id = US.schema_id
			ORDER BY base_schema_name, base_table_name
END

SELECT base_schema_name, base_table_name, constraint_name, base_column_name, unique_table_name, unique_column_name FROM @tbl_foreign_key_columns
";
            var dr = command.ExecuteReader();
            while (dr.Read())
            {
                var baseSchemaName = dr.GetValue(0).ToString().ToLower();
                var baseTableName = dr.GetValue(1).ToString().ToLower();
                var constraintName = dr.GetValue(2).ToString().ToLower();
                var baseColumnName = dr.GetValue(3).ToString().ToLower();
                var uniqueTableName = dr.GetValue(4).ToString().ToLower();
                var uniqueColumnName = dr.GetValue(5).ToString().ToLower();

                var pgTableName = baseSchemaName + "." + baseTableName;
                pgTableName = pgTableName.Replace(" ", "_");

                var pguniqueTableName = baseSchemaName + "." + uniqueTableName;
                pguniqueTableName = pguniqueTableName.Replace(" ", "_");

                var sql = string.Format(@"ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY({2}) REFERENCES {3} ({4});", pgTableName, constraintName, baseColumnName, pguniqueTableName, uniqueColumnName);
                Debug.Print(sql);
                pgStatements.Add(sql);
            }
        }
        return pgStatements.ToArray();
    }
}