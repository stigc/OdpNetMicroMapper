using System.Collections.Generic;
using System.Linq;

namespace OdpNetMicroMapper
{
    class SqlTokens
    {
        IDictionary<string, object> dic = new Dictionary<string, object>();

        public SqlTokens(IDictionary<string, object> dic)
        {
            this.dic = dic;
        }

        public IDictionary<string, object> GetNonNullableFieldsAndValues()
        {
            return dic.Where(x => x.Value != null)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        public string AsWhereClause(int index = 0)
        {
            string sql = "";
            foreach (string token in dic.Keys)
            {
                sql += token + "=:" + index + " and ";
                index++;
            }
            sql = "where " + sql.RemoveInEnd("and ");
            return sql;
        }

        public string AsColumnNames(bool includeNullFields = true)
        {
            int index = 0;
            string sql = "";
            foreach (string token in dic.Keys)
            {
                if (dic[token] != null || includeNullFields)
                {
                    sql += token.UnCammelCase() + ", ";
                    index++;
                }
            }
            sql = sql.RemoveInEnd(", ");
            return sql;
        }

        public string AsIndcies(bool includeNullFields = true)
        {
            int index = 0;
            string sql = "";
            foreach (string token in dic.Keys)
            {
                if (dic[token] != null || includeNullFields)
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
            foreach (string token in dic.Keys)
            {
                sql += token + "=:" + index + ", ";
                index++;
            }
            sql = sql.RemoveInEnd(", ");
            return sql;
        }
    }
}
