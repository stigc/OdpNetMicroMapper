using System.Collections.Generic;
using System.Linq;

namespace OdpNetMicroMapper
{
    public class SqlTokens
    {
        private readonly IDictionary<string, object> _dic;

        public SqlTokens(IDictionary<string, object> dic)
        {
            _dic = dic;
        }

        public IDictionary<string, object> GetNonNullableFieldsAndValues()
        {
            return _dic.Where(x => x.Value != null)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        public string AsWhereClause(int index = 0)
        {
            string sql = "";
            foreach (string token in _dic.Keys)
            {
                sql += token + "=:" + index + " and ";
                index++;
            }
            sql = "where " + sql.RemoveInEnd("and ");
            return sql;
        }

        public string AsColumnNames(bool includeNullFields = true)
        {
            string sql = "";
            foreach (string token in _dic.Keys)
            {
                if (_dic[token] != null || includeNullFields)
                {
                    sql += token.UnCammelCase() + ", ";
                }
            }
            sql = sql.RemoveInEnd(", ");
            return sql;
        }

        public string AsIndcies(bool includeNullFields = true)
        {
            int index = 0;
            string sql = "";
            foreach (string token in _dic.Keys)
            {
                if (_dic[token] != null || includeNullFields)
                {
                    sql += ":" + index + ", ";
                    index++;
                }
            }
            sql = sql.RemoveInEnd(", ");
            return sql;
        }

        public string AsSetClause(int index = 0)
        {
            string sql = "set ";
            foreach (string token in _dic.Keys)
            {
                sql += token + "=:" + index + ", ";
                index++;
            }
            sql = sql.RemoveInEnd(", ");
            return sql;
        }
    }
}
