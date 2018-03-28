using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Collections;

namespace OdpNetMicroMapper
{
    public static class ObjectExtensions
    {
        public static string RemoveInEnd(this string text, string search)
        {
            if (text.EndsWith(search))
                text = text.Substring(0, text.Length - search.Length);
            return text;
        }

        public static string UnCammelCase(this string name)
        {
            var s = "";

            foreach (var c in name)
            {
                if (char.IsUpper(c) && s.Length>0)
                    s += "_";
                s += char.ToLower(c);
            }

            return s;
        }

        private static bool IsUnderscoreAndIsReverseable(string columnName, int index)
        {
            //Last and first char are not reversable
            if (index == columnName.Length - 1 || index == 0)
                return false;

            if (columnName[index] == '_' && Char.ToUpper(columnName[index + 1]) != Char.ToLower(columnName[index + 1]))
                return true;

            return false;
        }

        public static string CammelCase(this string name)
        {
            var s = "";
            var nextIsToUpper = true;

            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];

                if (IsUnderscoreAndIsReverseable(name, i))
                {
                    nextIsToUpper = true;
                }
                else
                {
                    s += nextIsToUpper ? Char.ToUpper(c) : Char.ToLower(c);
                    nextIsToUpper = false;
                }
            }
            return s;
        }

        public static void SetPropertyValue(this object o, string name, object value)
        {
            var pi = o.GetType().GetProperty(name);
            pi.SetValue(o, value, null);
        }

        public static IDictionary<string, object> ToDictionary(this object o)
        {
            var dic = new Dictionary<string, object>();
            var props = o.GetType().GetProperties().OrderBy(x => x.MetadataToken);
            foreach (var item in props)
                dic.Add(item.Name, item.GetValue(o, null));
            return dic;
        }

        public static Entity ToEntity(this object o)
        {
            var dic = new Dictionary<string, object>();

            var props = o.GetType().GetProperties()
                //ignore collections
                .Where(x => x.PropertyType == typeof(string) || typeof(IEnumerable).IsAssignableFrom(x.PropertyType) == false)
                .ToList();

            foreach (var item in props)
                dic.Add(item.Name, item.GetValue(o, null));

            return new Entity(dic);
        }

        public static Entity ToEntity(this IDataReader rdr)
        {
            var dic = new Dictionary<string, object>();
            for (var i = 0; i < rdr.FieldCount; i++)
            {
                try
                {
                    dic.Add(rdr.GetName(i).CammelCase(), DBNull.Value.Equals(rdr[i]) ? null : rdr[i]);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException("Error reading column " + rdr.GetName(i), ex);
                }
            }
            return new Entity(dic);
        }

        public static Entity ToEntity(this DataRow row)
        {
            var dic = new Dictionary<string, object>();
            foreach (DataColumn c in row.Table.Columns)
                dic.Add(c.ColumnName.CammelCase(), DBNull.Value.Equals(row[c]) ? null : row[c]);
            return new Entity(dic);
        }

        public static T ToObject<T>(this IDataReader rdr) where T : new()
        {
            var o = new T();
            for (var i = 0; i < rdr.FieldCount; i++)
            {
                var name = rdr.GetName(i).CammelCase();
                var pi = o.GetType().GetProperty(name);
                if (pi != null)
                {
                    try
                    {
                        var type = Nullable.GetUnderlyingType(pi.PropertyType)
                                         ?? pi.PropertyType;

                        var value = DBNull.Value.Equals(rdr[i]) ? null :
                                        Convert.ChangeType(rdr[i], type);

                        pi.SetValue(o, value, null);
                    }
                    catch (Exception ex)
                    {
                        throw new ApplicationException("Error reading column " + rdr.GetName(i), ex);
                    }
                }
                else
                {
                    if (DbMapper.PrintWarnings)
                    {
                        Console.WriteLine("Warning: could not find property " + name);
                    }
                }
            }

            return o;
        }


    }
}
