﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Configuration;
using System.IO;

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
        private Type _oracleConnectionType, _oracleDataAdapterType, _oracleClobType;
        private Connection _connectionWhenCreateExternal;

        public DbMapper()
        {
            ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
            FindOracleDataTypes();
        }


        private void FindOracleDataTypes()
        {
            var prefix = "Oracle.ManagedDataAccess";
            var assembly = FindDataAccessAssembly(prefix);
            if (assembly == null)
            {
                prefix = "Oracle.DataAccess";
                assembly = FindDataAccessAssembly(prefix);
            }
            if (assembly == null) throw new FileNotFoundException($"Unable to load assembly {prefix}");

            _oracleDataAdapterType = TypeFromAssembly(assembly, $"{prefix}.Client.OracleDataAdapter");
            _oracleConnectionType = TypeFromAssembly(assembly, $"{prefix}.Client.OracleConnection");
            _oracleClobType = TypeFromAssembly(assembly, $"{prefix}.Types.OracleClob");
        }

        private Assembly FindDataAccessAssembly(string partialName)
        {
            try
            {
                return Assembly.Load(partialName);
            }
            catch
            {
                return null;
            }
        }

        private Type TypeFromAssembly(Assembly assembly, string typeAsString)
        {
            var type = assembly.GetType(typeAsString, false);
            return type;
        }

        /// <summary>
        /// Use this to establish a SYSDBA connection
        /// </summary>
        public void ConnectAsSys(string dataSource, string password)
        {
            ConnectionString = string.Format("data source={0}; user id=sys; password={1}; dba privilege=sysdba", dataSource, password);
        }

        public Connection SetExternalConnection(IDbConnection connection)
        {
            _connectionWhenCreateExternal = CreateOrReuseConnection(connection);
            return _connectionWhenCreateExternal;
        }

        public Connection OpenConnection()
        {
            if (_connectionWhenCreateExternal != null)
                throw new Exception("Cannot open new Connection. Already open");
            _connectionWhenCreateExternal = CreateOrReuseConnection();
            return _connectionWhenCreateExternal;
        }

        private Connection CreateOrReuseConnection(IDbConnection connection = null)
        {
            if (_connectionWhenCreateExternal != null)
            {
                _connectionWhenCreateExternal.NextLevel();
                return _connectionWhenCreateExternal;
            }

            if (connection == null)
            {
                connection = (IDbConnection)Activator.CreateInstance(_oracleConnectionType);
                connection.ConnectionString = ConnectionString;
                connection.Open();
            }
            return new Connection(this, connection);
        }

        internal void ReleaseConnection()
        {
            _connectionWhenCreateExternal = null;
        }

        public IDbDataAdapter CreateOracleDataAdapter()
        {
            return (IDbDataAdapter)Activator.CreateInstance(_oracleDataAdapterType);
        }

        public object CreateClob(string text, IDbConnection connection)
        {
            var clob = Activator.CreateInstance(_oracleClobType, connection);
            var method = _oracleClobType.GetMethod("Append", new[] { typeof(char[]), typeof(int), typeof(int) });
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

            if (sql != null)
                cmd.CommandText = sql;

            return cmd;
        }

        private void SetParameter(IDbDataParameter parameter, object value, IDbCommand cmd, Type type = null)
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
            var entity = item.ToEntity();
            entity.TableName = dbName;
            Insert(entity);
        }

        public void Insert(Entity item)
        {
            var sqlTokens = new SqlTokens(item.GetDictionaryInDbStyle(true));
            var sql = "insert into " + item.TableName
                + " (" + sqlTokens.AsColumnNames(false) + ") select " + sqlTokens.AsIndcies(false) + " from dual";

            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, sqlTokens.GetNonNullableFieldsAndValues(), 0);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void MergeInto(Entity item)
        {
            var dic = item.GetWhereClauseOnPrimaryKeyDbStyle();
            var args = dic.Values.ToArray();
            var whereClause = new SqlTokens(dic).AsWhereClause();

            var sql = "select count(1) from " + item.TableName + " " + whereClause;

            var count = QueryScalar<int>(sql, args);

            if (count == 0)
                Insert(item);
            else
                Update(item);
        }

        private void AddParameters(IDbCommand cmd, object[] args)
        {
            AddParameters(cmd, args.ToDictionary(x => Guid.NewGuid().ToString(), x => x), 0);
        }

        private void AddParameters(IDbCommand cmd, IDictionary<string, object> columnsAndValues, int offset, bool useKeyNames = false)
        {
            var index = offset;
            foreach (var o in columnsAndValues)
            {
                var parameter = cmd.CreateParameter();
                if (useKeyNames)
                    parameter.ParameterName = o.Key;
                else
                    parameter.ParameterName = index.ToString();
                if (o.Key.EndsWith("Output"))
                    parameter.Direction = ParameterDirection.Output;
                SetParameter(parameter, o.Value, cmd);
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
                var sqlTokens = new SqlTokens(dic);
                whereClause = sqlTokens.AsWhereClause();
                args = dic.Values.ToArray();
            }

            var sql = "delete from " + item.TableName + " " + whereClause;

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

            var setClauseTokens = new SqlTokens(nonPrimaryKeysColumns);

            var sql = "update " + item.TableName
                + " " + setClauseTokens.AsSetClause(args.Length)
                + " " + whereClause;

            using (var connection = CreateOrReuseConnection())
            {
                using (var command = CreateCommand(sql, connection.GetAdoConnection()))
                {
                    AddParameters(command, args);
                    AddParameters(command, item.GetDictionaryInDbStyle(false), args.Length);
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
                    var result = command.ExecuteScalar();


                    var isString = typeof(T) == typeof(string);
                    var isNullableType = typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>).GetGenericTypeDefinition();

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
                        var type = Nullable.GetUnderlyingType(typeof(T));
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
                    catch (Exception ex)
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

                    using (var reader = command.ExecuteReader())
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

                    using (var reader = command.ExecuteReader())
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

                    using (var reader = command.ExecuteReader())
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

        public T ExecuteFunction<T>(string functionName, params object[] args)
        {
            using (var connection = CreateOrReuseConnection())
            {
                using (var cmd = CreateCommand(null, connection.GetAdoConnection(), false))
                {
                    cmd.CommandText = functionName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    //return value
                    var parameter = cmd.CreateParameter();
                    SetParameter(parameter, default(T), cmd, typeof(T));
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
                    var ds = new DataSet();
                    command.CommandText = procedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    AddParameters(command, args);

                    //Execute
                    var da = CreateOracleDataAdapter();
                    da.SelectCommand = command;
                    da.Fill(ds);

                    if (ds.Tables.Count < 1)
                        return null;

                    var list = new List<dynamic>();

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
        /// </summary>
        public void SetParameterOracleDbType(IDbDataParameter parameter, string type)
        {
            var pOracleDbType = parameter.GetType().GetProperty("OracleDbType");

            var enums = Enum.Parse(pOracleDbType.PropertyType, type);

            pOracleDbType.SetValue(parameter, enums, null);
        }
    }
}