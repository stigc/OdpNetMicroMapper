using System;
using System.Data;

namespace OdpNetMicroMapper
{
    public class Connection : IDisposable
    {
        private int _level;
        private readonly IDbConnection _connection;
        private readonly DbMapper _orm;
        public IDbConnection GetAdoConnection()
        {
            return _connection;
        }

        public Connection(DbMapper orm, IDbConnection connection)
        {
            _orm = orm;
            _connection = connection;
        }

        public void NextLevel()
        {
            _level++; 
        }

        public IDbTransaction BeginTransaction()
        {
            return _connection.BeginTransaction();
        }

        public void Dispose()
        {
            if (_level == 0)
            {
                _connection.Dispose();
                _orm.ReleaseConnection();
            }
            else
            {
                _level--;
            }
        }
    }
}
