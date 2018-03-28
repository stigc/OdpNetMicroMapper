using System;
using System.Collections.Generic;
using System.Linq;
using System.Dynamic;

namespace OdpNetMicroMapper
{
    public class Entity : DynamicObject
    {
        private readonly string _dbPrimaryKey;
        public string TableName { get; set; }
        private readonly Dictionary<string, object> _dictionary = new Dictionary<string, object>();

        public Entity(Dictionary<string, object> dictionary)
        {
            _dictionary = dictionary;
        }

        public Entity(string tableName, string dbPrimaryKey = null)
        {
            TableName = tableName;
            _dbPrimaryKey = dbPrimaryKey;
        }

        private bool IsPrimaryKey(string propertyName)
        {
            if (_dbPrimaryKey == null)
                return false;

            return _dbPrimaryKey.Split(new[] { ',' })
                .Select(x => x.Trim())
                .Contains(propertyName);
        }

        public IDictionary<string, object> GetDictionary()
        {
            return _dictionary;
        }

        public Dictionary<string, object> GetDictionaryInDbStyle(bool includePrimaryKey)
        {
            var dic = new Dictionary<string, object>();
            foreach (var element in _dictionary)
                if (includePrimaryKey || !IsPrimaryKey(element.Key.UnCammelCase()))
                    dic.Add(element.Key.UnCammelCase(), element.Value);
            return dic;
        }

        public Dictionary<string, object> GetWhereClauseOnPrimaryKeyDbStyle()
        {
            if (_dbPrimaryKey == null)
                throw new ApplicationException("Primary key is not set");

            var dic = new Dictionary<string, object>();

            foreach (var column in _dbPrimaryKey.Split(new[] { ',' }))
            {
                var domainProperty = column.Trim().CammelCase();
                if (!_dictionary.ContainsKey(domainProperty))
                    throw new ApplicationException("Property '" + domainProperty + "' was not found");
                dic.Add(column, _dictionary[domainProperty]);
            }
            return dic;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            _dictionary[binder.Name] = value;
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return _dictionary.TryGetValue(binder.Name, out result);
        }

        public override string ToString()
        {
            var s = "";

            foreach (var element in _dictionary)
                s += element.Key + " = " + element.Value + "\n";

            return s;
        }
    }
}
