using System;
using System.Collections.Generic;
using System.Linq;
using System.Dynamic;

namespace OdpNetMicroMapper
{
    public class Entity : DynamicObject
    {
        string dbPrimaryKey;
        public string TableName { get; set; }
        Dictionary<string, object> dictionary = new Dictionary<string, object>();

        public Entity(Dictionary<string, object> dictionary)
        {
            this.dictionary = dictionary;
        }

        public Entity(string tableName, string dbPrimaryKey = null)
        {
            TableName = tableName;
            this.dbPrimaryKey = dbPrimaryKey;
        }

        private bool IsPrimaryKey(string propertyName)
        {
            if (dbPrimaryKey == null)
                return false;

            return dbPrimaryKey.Split(new char[] { ',' })
                .Select(x => x.Trim())
                .Contains(propertyName);
        }

        public IDictionary<string, object> GetDictionary()
        {
            return dictionary;
        }

        public Dictionary<string, object> GetDictionaryInDbStyle(bool includePrimaryKey)
        {
            Dictionary<string, object> dic = new Dictionary<string, object>();
            foreach (KeyValuePair<string, object> element in dictionary)
                if (includePrimaryKey || !IsPrimaryKey(element.Key.UnCammelCase()))
                    dic.Add(element.Key.UnCammelCase(), element.Value);
            return dic;
        }

        public Dictionary<string, object> GetWhereClauseOnPrimaryKeyDbStyle()
        {
            if (dbPrimaryKey == null)
                throw new ApplicationException("Primary key is not set");

            var dic = new Dictionary<string, object>();

            foreach (string column in dbPrimaryKey.Split(new char[] { ',' }))
            {
                string domainProperty = column.Trim().CammelCase();
                if (!dictionary.ContainsKey(domainProperty))
                    throw new ApplicationException("Property '" + domainProperty + "' was not found");
                dic.Add(column, dictionary[domainProperty]);
            }
            return dic;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            dictionary[binder.Name] = value;
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return dictionary.TryGetValue(binder.Name, out result);
        }

        public override string ToString()
        {
            string s = "";

            foreach (KeyValuePair<string, object> element in dictionary)
                s += element.Key + " = "+ element.Value + "\n";

            return s;
        }
    }
}
