using System;
using System.Data;

namespace OdpNetMicroMapper
{
    public class Connection : IDisposable
    {
        int level = 0;
        IDbConnection connection;
        DbMapper orm;
        public IDbConnection GetAdoConnection()
        {
            return connection;
        }

        public Connection(DbMapper orm, IDbConnection connection)
        {
            this.orm = orm;
            this.connection = connection;
        }

        public void NextLevel()
        {
            level++; 
        }

        public IDbTransaction BeginTransaction()
        {
            return connection.BeginTransaction();
        }


        public void Dispose()
        {
            if (level == 0)
            {
                connection.Dispose();
                orm.ReleaseConnection();
            }
            else
            {
                level--;
            }
        }
    }
}
