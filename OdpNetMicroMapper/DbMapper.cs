using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Configuration;

namespace OdpNetMicroMapper
{
    public enum DbMapperParameter
    {
        RefCursor
    }

    public class DbMapper
    {
        public static volatile bool PrintWarnings = false;
        public static volatile bool PrintSqls = false;

        public string ConnectionString { get; set; }
        Type oracleConnectionType, oracleDataAdapterType;
        Connection connectionWhenCreateExternal;
        
        public DbMapper()
        {
            ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];

            oracleDataAdapterType = TypeFromAssembly("Oracle.DataAccess.Client.OracleDataAdapter");
            oracleConnectionType = TypeFromAssembly("Oracle.DataAccess.Client.OracleConnection");
        }

        private Type TypeFromAssembly(string typeAsString)
        {
            Assembly assembly = Assembly.Load("Oracle.DataAccess");
            Type type = assembly.GetType(typeAsString, false);
            return type;
        }

        /// <summary>
        /// Use this to establish a SYSDBA connection
        /// </summary>
        /// <param name="tns"></param>
        public void ConnectAsSys(string dataSource, string password)
        {
            ConnectionString = String.Format("data source={0}; user id=sys; password={1}; dba privilege=sysdba", dataSource, password);
        }

        public Connection SetExternalConnection(IDbConnection connection)
        {
            connectionWhenCreateExternal = CreateOrReuseConnection(connection);
            return connectionWhenCreateExternal;
        }

        public Connection OpenConnection()
        {
            if (connectionWhenCreateExternal != null)
                throw new Exception("Cannot open new Connection. Already open");
            connectionWhenCreateExternal = CreateOrReuseConnection();
            return connectionWhenCreateExternal;
        }

        private Connection CreateOrReuseConnection(IDbConnection connection = null)
        {
            if (connectionWhenCreateExternal != null)
            {
                connectionWhenCreateExternal.NextLevel();
                return connectionWhenCreateExternal;
            }

            if (connection == null)
            {
                connection = (IDbConnection)Activator.CreateInstance(oracleConnectionType);
                connection.ConnectionString = ConnectionString;
                connection.Open();
            }
            return new Connection(this, connection);
        }

        internal void ReleaseConnection()
        {
            connectionWhenCreateExternal = null;
        }

        public IDbDataAdapter CreateOracleDataAdapter()
        {
            return (IDbDataAdapter)Activator.CreateInstance(oracleDataAdapterType);
        }

        public object CreateClob(string text, IDbConnection connection)
        {
            var oracleClobType = TypeFromAssembly("Oracle.DataAccess.Types.OracleClob");
            object clob = Activator.CreateInstance(oracleClobType, new object[] { connection });
            MethodInfo method = oracleClobType.GetMethod("Append", new Type[] { typeof(char[]), typeof(int), typeof(int) });
            method.Invoke(clob, new object[] { text.ToCharArray(), 0, text.Length });
            return clob;
        }

        public IDbCommand CreateCommand(string sql, IDbConnection connection, bool bindByName = true)
        {
            if (PrintSqls)
                Console.WriteLine(sql);

            var cmd = connection.CreateCommand();

            if (bindByName)
                cmd.GetType().GetProperty("BindByName")
                    .SetValue(cmd, true, null);

            cmd.GetType().GetProperty("InitialLONGFetchSize")
                .SetValue(cmd, 1024 * 64, null); //reade up to 64kb with long columns

            if (sql!=null)
                cmd.CommandText = sql;

            return cmd;
        }

        private void SetParameter(IDbDataParameter parameter, object value, string columnName, IDbCommand cmd, Type type = null)
        {
            if (value == null)
            {
                parameter.Value = DBNull.Value;
                if (type != null)
                {
                    if (type == typeof(string))
                    {
                        parameter.DbType = DbType.String;
                        parameter.Size = 4096;
                    }
                }
            }
            else
            {
                if (value is float || value is double || value is Decimal)
                {
                    parameter.DbType = DbType.Decimal;
                    parameter.Value = Convert.ToDecimal(value);
                }
                else if (value is int || value is bool)
                {
                    parameter.DbType = DbType.Int32; //ok?
                    parameter.Value = Convert.ToInt32(value);
                }
                else if (value is DateTime)
                {
                    parameter.DbType = DbType.DateTime;
                    parameter.Value = Convert.ToDateTime(value);
                }
                //todo: check for real enum value
                else if (value is DbMapperParameter)
                {
                    parameter.Direction = ParameterDirection.Output;
                    SetParameterOracleDbType(parameter, "RefCursor");
                }
                else if (value is byte[])
                {
                    SetParameterOracleDbType(parameter, "Raw");
                    parameter.Value = value;
                }
                //strings
                else
                {
                    if (((string)value).Length > 4000)
                    {
                        SetParameterOracleDbType(parameter, "Clob");
                        parameter.Value = CreateClob(value.ToString(), cmd.Connection);
                    }
                    else
                    {
                        parameter.DbType = DbType.String;
                        parameter.Value = Convert.ToString(value);
                    }
                }
            }
        }

        public void Insert(object item, string dbName)
        {
            Entity entity = item.ToEntity();
            entity.TableName = dbName;
            Insert(entity);
        }

        public void Insert(Entity item)
        {
            SqlTokens sqlTokens = new SqlTokens(item.GetDictionaryInDbStyle(true));
            string sql = "insert into " + item.TableName
                + " (" + sqlTokens.AsColumnNames(false) + ") select " + sqlTokens.AsIndcies(false) + " from dual";

            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, sqlTokens.GetNonNullableFieldsAndValues(), connection.GetAdoConnection(), 0);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void MergeInto(Entity item)
        {
            var dic = item.GetWhereClauseOnPrimaryKeyDbStyle();
            var args = dic.Values.ToArray();
            string whereClause = new SqlTokens(dic).AsWhereClause();

            string sql = "select count(1) from " + item.TableName + " " + whereClause;

            int count = QueryScalar<int>(sql, args);

            if (count == 0)
                Insert(item);
            else
                Update(item);
        }

        private void AddParameters(IDbCommand cmd, object[] args)
        {
            AddParameters(cmd, args.ToDictionary(x => Guid.NewGuid().ToString(), x => x), cmd.Connection, 0);
        }

        private void AddParameters(IDbCommand cmd, IDictionary<string, object> columnsAndValues, IDbConnection connection, int offset, bool useKeyNames = false)
        {
            int index = offset;
            foreach (KeyValuePair<string, object> o in columnsAndValues)
            {
                var parameter = cmd.CreateParameter();
                if (useKeyNames)
                    parameter.ParameterName = o.Key;
                else
                    parameter.ParameterName = index.ToString();
                if (o.Key.EndsWith("Output"))
                    parameter.Direction = ParameterDirection.Output;
                SetParameter(parameter, o.Value, o.Key, cmd);
                cmd.Parameters.Add(parameter);
                index++;
            }
        }

        public void Delete(Entity item, string whereClause = null, params object[] args)
        {
            //build where clause from metadata
            if (whereClause == null)
            {
                var dic = item.GetWhereClauseOnPrimaryKeyDbStyle();
                SqlTokens sqlTokens = new SqlTokens(dic);
                whereClause = sqlTokens.AsWhereClause();
                args = dic.Values.ToArray();
            }

            string sql = "delete from " + item.TableName + " " + whereClause;

            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void Update(Entity item, string whereClause = null, params object[] args)
        {
            //build where/set clause from metadata
            if (whereClause == null)
            {
                var dic = item.GetWhereClauseOnPrimaryKeyDbStyle();
                args = dic.Values.ToArray();
                whereClause = new SqlTokens(dic).AsWhereClause();
            }

            var nonPrimaryKeysColumns = item.GetDictionaryInDbStyle(false);

            if (nonPrimaryKeysColumns.Count == 0)
                return;

            SqlTokens setClauseTokens = new SqlTokens(nonPrimaryKeysColumns);

            string sql = "update " + item.TableName 
                + " " + setClauseTokens.AsSetClause(args.Length)
                + " " + whereClause;

            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);
                    AddParameters(command, item.GetDictionaryInDbStyle(false), null, args.Length);
                    command.ExecuteNonQuery();
                }
            }
        }

        public T QueryScalar<T>(string sql, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);
                    object result = command.ExecuteScalar();


                    bool isString = typeof(T) == typeof(string);
                    bool isNullableType = typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>).GetGenericTypeDefinition();

                    //NULL handle
                    if (isNullableType || isString)
                    {
                        if (result == DBNull.Value || result == null)
                        {
                            return default(T); //this is mostly null :)
                        }
                    }

                    if (isNullableType)
                    {
                        Type type = Nullable.GetUnderlyingType(typeof(T));
                        return (T)Convert.ChangeType(result, type);
                    }

                    return (T)Convert.ChangeType(result, typeof(T));
                }
            }
        }

        public void NonQueryIgnoreError(string sql, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);

                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine("Db Exception ignored: " + ex.Message);
                    }
                }
            }
        }

        public void NonQuery(string sql, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);
                    command.ExecuteNonQuery();
                }
            }
        }

        public IEnumerable<T> Query<T>(string sql, params object[] args) where T : new()
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);

                    using (IDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader.ToObject<T>();
                        }
                    }
                }
            }
        }

        public IEnumerable<dynamic> Query(string sql, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);

                    using (IDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader.ToEntity();
                        }
                    }
                }
            }
        }

        public IEnumerable<T> QuerySingleTypeList<T>(string sql, params object[] args) 
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);

                    using (IDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return (T)Convert.ChangeType(reader.GetValue(0), typeof(T));
                        }
                    }
                }
            }
        }

        public IEnumerable<T> ExecuteProcedure<T>(string procedureName, params object[] args) where T : new()
        {
            dynamic d = ExecuteProcedure(procedureName, args);
            return d.RecordToObject<T>();
        }


        //TODO: SMELLY, delete?
        public IEnumerable<dynamic> ExecuteProcedure2(string procedureName, object parameters)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(null, connection.GetAdoConnection(), false))
                {
                    DataSet ds = new DataSet();
                    command.CommandText = procedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    AddParameters(command, parameters.ToDictionary(), null, 0, true);

                    //Execute
                    IDbDataAdapter da = CreateOracleDataAdapter();
                    da.SelectCommand = command;
                    da.Fill(ds);

                    //Check for out parameters
                    foreach (IDbDataParameter p in command.Parameters)
                    {
                        if (p.Direction == ParameterDirection.Output
                            && p.Value != DBNull.Value)
                            parameters.SetPropertyValue(p.ParameterName, p.Value);
                    }

                    List<dynamic> list = new List<dynamic>();

                    foreach (DataRow row in ds.Tables[0].Rows)
                        list.Add(row.ToEntity());

                    return list;
                }
            }
        }

        public T ExecuteFunction<T>(string functionName, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var cmd = CreateCommand(null, connection.GetAdoConnection(), false))
                {
                    DataSet ds = new DataSet();
                    cmd.CommandText = functionName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    //return value
                    var parameter = cmd.CreateParameter();
                    SetParameter(parameter, default(T), "dummy", cmd, typeof(T));
                    parameter.Direction = ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(parameter);

                    AddParameters(cmd, args);
                    cmd.ExecuteNonQuery();

                    return (T)Convert.ChangeType(parameter.Value, typeof(T));
                }
            }
        }

        public IEnumerable<dynamic> ExecuteProcedure(string procedureName, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(null, connection.GetAdoConnection(), false))
                {
                    DataSet ds = new DataSet();
                    command.CommandText = procedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    AddParameters(command, args);

                    //Execute
                    IDbDataAdapter da = CreateOracleDataAdapter();
                    da.SelectCommand = command;
                    da.Fill(ds);
                    
                    if (ds.Tables.Count < 1)
                        return null;

                    List<dynamic> list = new List<dynamic>();
                    
                    foreach (DataRow row in ds.Tables[0].Rows)
                        list.Add(row.ToEntity());
                    return list;
                }
            }
        }

        /// <summary>
        /// Set OracleDbType on a OracleParameter without a reference to Oracle DataAccess
        /// <param name="parameter"></param>
        /// <param name="type">Clob, Blob etc.</param>
        public void SetParameterOracleDbType(IDbDataParameter parameter, string type)
        {
            var pOracleDbType = parameter.GetType().GetProperty("OracleDbType");

            var enums = Enum.Parse(pOracleDbType.PropertyType, type);

            pOracleDbType.SetValue(parameter, enums, null);
        }
    }
}