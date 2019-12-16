using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using OdpNetMicroMapper;
using Oracle.ManagedDataAccess.Client;

namespace TestManaged
{
    [TestFixture]
    public class Test : TestBase
    {
        [Test]
        public void InsertStatic()
        {
            Item item = new Item();
            item.Id = 3;
            item.Name = "Third Item";
            orm.Insert(item, "onmm2.item");

            item = orm.Query<Item>("select * from onmm2.item where id = :0", 3).SingleOrDefault();
            Assert.AreEqual(3, item.Id);
            Assert.AreEqual("Third Item", item.Name);
            Assert.AreEqual(DateTime.Today, item.DateValue.Value.Date);
        }

        [Test]
        public void RawTest()
        {
            var guid = new Guid();
            orm.NonQuery("insert into onmm2.rawtest (bytes) values (:0)", guid.ToByteArray());

            var bytesLoaded = orm.QueryScalar<byte[]>("select bytes from onmm2.rawtest where rownum = 1");
            var guidLoaded = new Guid(bytesLoaded);
            Assert.AreEqual(guid, guidLoaded);

            dynamic item = orm.Query("select bytes from onmm2.rawtest where rownum = 1").Single();
            Assert.AreEqual(guid, new Guid(item.Bytes));
        }

        [TestCase("double")]
        [TestCase("float")]
        [TestCase("decimal")]
        public void InsertDynamicTest(string type)
        {
            dynamic item = new Entity("onmm2.item");
            item.Id = 3;
            item.Name = "Third Item";
            item.DateValue = DateTime.Today;
            if (type == "double") item.DecimalValue = 3.1415d;
            if (type == "float") item.DecimalValue = 3.1415f;
            if (type == "decimal") item.DecimalValue = 3.1415m;

            orm.Insert(item);

            item = orm.Query("select * from onmm2.item where id = :0", 3).Single();
            Assert.AreEqual(3, item.Id);
            Assert.AreEqual(DateTime.Today, item.DateValue);
            Assert.AreEqual("Third Item", item.Name);
            Assert.AreEqual(DateTime.Today, item.DateValue.Date);
            Assert.AreEqual(3.1415m, item.DecimalValue);
        }

        [Test]
        public void UpdateDynamicWithNull()
        {
            dynamic item = new Entity("onmm2.item", "id");
            item.Id = 1;
            item.Name = null;

            orm.Update(item);

            item = orm.Query("select * from onmm2.item where id = :0", 1).Single();
            Assert.AreEqual(1, item.Id);
            Assert.IsNull(item.Name);
        }

        [Test]
        public void InsertWithLongColumnMax4000chars()
        {
            dynamic item = new Entity("onmm2.item_with_long");
            item.Text = new string('X', 1000 * 4); //max 4k when inserting!
            orm.Insert(item);

            var items = orm.Query("select * from onmm2.item_with_long");
            Assert.AreEqual(item.Text, items.Single().Text);
        }

        [Test]
        public void DeleteWithWhereClause()
        {
            dynamic item = new Entity("onmm2.item");
            Assert.AreEqual(2, orm.QueryScalar<int>("select count(1) from onmm2.item"));

            orm.Delete(item, "where id = :0", 1);
            Assert.AreEqual(1, orm.QueryScalar<int>("select count(1) from onmm2.item"));

            orm.Delete(item, "where id = :0", 2);
            Assert.AreEqual(0, orm.QueryScalar<int>("select count(1) from onmm2.item"));
        }

        [Test]
        public void DeleteWithKeyMetadata()
        {
            dynamic item = new Entity("onmm2.item", "id");
            item.Id = 2;
            Assert.AreEqual(2, orm.QueryScalar<int>("select count(1) from onmm2.item"));

            orm.Delete(item);
            Assert.AreEqual(1, orm.QueryScalar<int>("select count(1) from onmm2.item"));

            //check that the existing element is not the one deleted.
            var items = orm.Query("select * from onmm2.item");
            Assert.AreEqual(1, items.Single().Id);
        }

        [Test]
        public void QueryScalarWhenNoRows()
        {
            Assert.AreEqual(null, orm.QueryScalar<int?>("select 1 from onmm2.item where 1=2"));
        }

        [Test]
        public void QueryScalarNullable()
        {
            Assert.AreEqual(null, orm.QueryScalar<byte?>("select null from dual"));
            Assert.AreEqual(null, orm.QueryScalar<int?>("select null from dual"));
            Assert.AreEqual(null, orm.QueryScalar<long?>("select null from dual"));
            Assert.AreEqual(null, orm.QueryScalar<decimal?>("select null from dual"));
            Assert.AreEqual(null, orm.QueryScalar<string>("select null from dual"));
        }

        [Test]
        public void QueryScalar()
        {
            Assert.AreEqual(1, orm.QueryScalar<byte>("select 1 from dual"));
            Assert.AreEqual(1, orm.QueryScalar<int>("select 1 from dual"));
            Assert.AreEqual(1, orm.QueryScalar<long>("select 1 from dual"));
            Assert.AreEqual(1, orm.QueryScalar<decimal>("select 1 from dual"));
            Assert.AreEqual("1", orm.QueryScalar<string>("select 1 from dual"));

            Assert.AreEqual(DateTime.Today, orm.QueryScalar<DateTime>("select trunc(sysdate) from dual"));
            Assert.AreEqual("test", orm.QueryScalar<string>("select 'test' from dual"));
            Assert.AreEqual(1.123456789m, orm.QueryScalar<decimal>("select 1.123456789 from dual"));
        }

        [Test]
        public void UpdateViaWhereClause()
        {
            dynamic item = new Entity("onmm2.item");
            item.Name = "RENAMED";
            orm.Update(item, "where id = :0", 1);

            var items = orm.Query("select id, name from onmm2.item order by id");
            Assert.AreEqual(items.First().Name, item.Name);
            Assert.AreNotEqual(items.Last().Name, item.Name);
        }

        [Test]
        public void UpdateViaMetaData()
        {
            dynamic item = new Entity("onmm2.item", "id");
            item.Id = 1;
            item.Name = "RENAMED";
            orm.Update(item);

            var items = orm.Query("select id, name from onmm2.item order by id");
            Assert.AreEqual(items.First().Name, item.Name);
            Assert.AreNotEqual(items.Last().Name, item.Name);
        }

        [Test]
        public void MergeInto()
        {
            dynamic item = new Entity("onmm2.item", "id");
            item.Id = 100;
            item.Name = "Name100";
            orm.MergeInto(item);

            var itemLoaded = orm.Query<Item>("select * from onmm2.item where id = :0", 100).SingleOrDefault();
            Assert.AreEqual(item.Name, itemLoaded.Name);

            item.Name = "renamed";
            orm.MergeInto(item);

            itemLoaded = orm.Query<Item>("select * from onmm2.item where id = :0", 100).SingleOrDefault();
            Assert.AreEqual(item.Name, itemLoaded.Name);
        }

        [Test]
        public void MergeIntoWithOnlyPrimaryKey_ShouldNotTryUpdateAndFail()
        {
            dynamic item = new Entity("onmm2.item", "id");
            item.Id = 100;
            orm.MergeInto(item);
            orm.MergeInto(item);
        }

        [Test]
        public void ColumnWithUnderScoreBeforeDigit()
        {
            Entity item = orm.Query("select yield_2date from onmm2.item_odd where id = 1").Single();

            //Assert.AreEqual(99m, item.Yield_2date);

            foreach (var p in item.GetDictionaryInDbStyle(true))
                Assert.AreEqual("yield_2date", p.Key);

            foreach (var p in item.GetDictionary())
                Assert.AreEqual("Yield_2date", p.Key);
        }


        [Test]
        public void QueryDynamicToString()
        {
            var item = orm.Query("select * from onmm2.item where id = 1").Single();
            Assert.That(item.ToString(), Contains.Substring("Id = 1"));
            Assert.That(item.ToString(), Contains.Substring("Name = First Item"));
        }

        [Test]
        public void QuerySingleTypeList()
        {
            var ints = orm.QuerySingleTypeList<int>("select id from onmm2.item order by id").ToList();
            Assert.AreEqual(1, ints[0]);
            Assert.AreEqual(2, ints[1]);

            var strings = orm.QuerySingleTypeList<string>("select name from onmm2.item order by id").ToList();
            Assert.AreEqual("First Item", strings[0]);
            Assert.AreEqual("Second Item", strings[1]);
        }

        [Test]
        public void QueryStaticNoWhereClause()
        {
            var items = orm.Query<Item>("select * from onmm2.item order by id").ToList();
            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(1, items[0].Id);
            Assert.AreEqual(2, items[1].Id);
            Assert.AreEqual("First Item", items[0].Name);
            Assert.AreEqual("Second Item", items[1].Name);
            Assert.AreEqual(0.321m, items[0].DecimalValue);
            Assert.AreEqual(.123m, items[1].DecimalValue);
        }

        [Test]
        public void QueryDynamicWithWhereClause()
        {
            var item = orm.Query("select id, name from onmm2.item where id = :0", 1).SingleOrDefault();
            Assert.AreEqual(1, item.Id);
            Assert.AreEqual("First Item", item.Name);
        }

        [Test]
        public void QueryStaticWithWhereClause()
        {
            var item = orm.Query<Item>("select id, name from onmm2.item where id = :0", 1)
                .SingleOrDefault();
            Assert.AreEqual(1, item.Id);
            Assert.AreEqual("First Item", item.Name);
        }

        [Test]
        public void QueryNonExisting()
        {
            var item = orm.Query<Item>("select id, name from onmm2.item where id = :0", -1)
                .SingleOrDefault();
            Assert.IsNull(item);
        }


        [Test]
        public void ExecuteFunctionWithInt()
        {
            var result = orm.ExecuteFunction<int>("onmm2.plus1function", 100);
            Assert.AreEqual(101, result);
        }

        [Test]
        public void ExecuteFunctionWithString()
        {
            var result = orm.ExecuteFunction<string>("onmm2.append1function", "100");
            Assert.AreEqual("1001", result);
        }

        [Test]
        public void ExecuteProcedureWithRefCursor()
        {
            var item = orm.ExecuteProcedure("onmm2.get_items_by_name", "First Item", DbMapperParameter.RefCursor)
                .SingleOrDefault();
            Assert.AreEqual(1, item.Id);
            Assert.AreEqual("First Item", item.Name);
        }

        [Test]
        public void ExecuteProcedureWithRefCursorZeroElements()
        {
            var items = orm.ExecuteProcedure("onmm2.get_items_by_name", "non existing", DbMapperParameter.RefCursor);
            Assert.AreEqual(0, items.Count());
        }

        [Test]
        public void ExecuteProcedureWithoutRefCursor()
        {
            string newName = "RENAMED";
            object result = orm.ExecuteProcedure("onmm2.rename_item", 1, newName);
            Assert.IsNull(result);
            Assert.AreEqual(newName, orm.QueryScalar<string>("select name from onmm2.item where id = :0", 1));
        }

        [Test]
        public void InsertWithImplicitConnection()
        {
            var sw = new Stopwatch();
            sw.Start();

            int count = 1000;
            dynamic item = new Entity("onmm2.item", "id");
            item.id = 1;

            for (int i = 0; i < count; i++)
                orm.Insert(item);

            Assert.AreEqual(count + 2, orm.QueryScalar<int>("select count(1) from onmm2.item"));
            Console.WriteLine("InsertWithImplicitConnection Ms used: " + sw.ElapsedMilliseconds);
        }

        [Test]
        public void InsertWithExplicitConnection()
        {
            var sw = new Stopwatch();
            sw.Start();

            int count = 1000;
            dynamic item = new Entity("onmm2.item", "id");
            item.id = 1;

            using (var connection = orm.OpenConnection())
            {
                for (int i = 0; i < count; i++)
                    orm.Insert(item);
            }

            Assert.AreEqual(count + 2, orm.QueryScalar<int>("select count(1) from onmm2.item"));
            Console.WriteLine("InsertWithExplicitConnection Ms used: " + sw.ElapsedMilliseconds);
        }

        [Test]
        public void InsertWithExplicitConnectionAndTransaction()
        {
            var sw = new Stopwatch();
            sw.Start();

            int count = 1000;
            dynamic item = new Entity("onmm2.item", "id");
            item.id = 1;

            using (var connection = orm.OpenConnection())
            using (var tx = connection.BeginTransaction())
            {
                for (int i = 0; i < count; i++)
                    orm.Insert(item);
                tx.Commit();
            }

            Assert.AreEqual(count + 2, orm.QueryScalar<int>("select count(1) from onmm2.item"));
            Console.WriteLine("InsertWithExplicitConnectionAndTransaction Ms used: " + sw.ElapsedMilliseconds);
        }

        [Test]
        public void InsertAndSelectBigClob()
        {
            //this is at least 5 MB
            string largeString = new string('X', 1024 * 1024 * 5);

            //insert
            dynamic item = new Entity("onmm2.bigclobtest");
            item.Text = largeString;
            orm.Insert(item);

            //select
            item = orm.Query("select * from onmm2.bigclobtest").SingleOrDefault();
            Assert.AreEqual(largeString, item.Text);

            //scalar
            string largeStringFetched = orm.QueryScalar<string>("select text from onmm2.bigclobtest");
            Assert.AreEqual(largeString, largeStringFetched);
        }


        [Test]
        public void InsertBigClobWithNonQuery()
        {
            //this is at least 5 MB
            string largeString = new string('X', 1024 * 1024 * 5);

            orm.NonQuery("insert into onmm2.bigclobtest (text) values (:0)", largeString);

            string largeStringFetched = orm.QueryScalar<string>("select text from onmm2.bigclobtest");
            Assert.AreEqual(largeString, largeStringFetched);
        }

        [Test]
        public void SelectWithExternalConnection()
        {
            Console.WriteLine("InsertWithExternalConnectionAndSelectBigClob");

            using (OracleConnection oracleConnection = new OracleConnection(orm.ConnectionString))
            {
                oracleConnection.Open();
                using (var conncetion = orm.SetExternalConnection(oracleConnection))
                {
                    var r = orm.QueryScalar<int>("select 1 from dual");
                    Assert.AreEqual(1, r);
                }
            }
        }

        [Test]
        public void InsertWithExternalConnectionAndSelectBigClob()
        {
            Console.WriteLine("InsertWithExternalConnectionAndSelectBigClob");

            using (OracleConnection oracleConnection = new OracleConnection(orm.ConnectionString))
            {
                oracleConnection.Open();
                using (var conncetion = orm.SetExternalConnection(oracleConnection))
                {
                    //this is at least 5 MB
                    string largeString = new string('X', 1024 * 1024 * 1);

                    //insert
                    dynamic item = new Entity("onmm2.bigclobtest");
                    item.Text = largeString;
                    orm.Insert(item);

                    //select
                    item = orm.Query("select * from onmm2.bigclobtest").SingleOrDefault();
                    Assert.AreEqual(largeString, item.Text);

                    //scalar
                    string largeStringFetched = orm.QueryScalar<string>("select text from onmm2.bigclobtest");
                    Assert.AreEqual(largeString, largeStringFetched);
                }
            }
        }

        [Test]
        public void DeleteWithCompositeKeyt()
        {
            dynamic item1 = new Entity("onmm2.item_composite_key", "id, type");
            item1.Id = 1;
            item1.Type = 1;
            orm.Insert(item1);

            dynamic item2 = new Entity("onmm2.item_composite_key", "id,type");
            item2.Id = 1;
            item2.Type = 2;
            orm.Insert(item2);

            orm.Delete(item1);
            var list = orm.Query("select * from onmm2.item_composite_key");
            Assert.AreEqual(2, list.Single().Type);

            orm.Delete(item2);
            Assert.AreEqual(0, orm.QueryScalar<int>("select count(1) from onmm2.item_composite_key"));
        }

        [Test]
        public void OverflowDynamic()
        {
            Assert.Throws<ApplicationException>(() => orm.Query("select 1/3 decimal_value from dual").Single());
            Assert.Throws<ApplicationException>(() => orm.Query("select 999999999999999999999999999999 decimal_value from dual").Single());
        }

        [Test]
        public void Overflow()
        {
            Assert.Throws<ApplicationException>(() => orm.Query<Item>("select 1/3 decimal_value from dual").Single());
            Assert.Throws<ApplicationException>(() => orm.Query<Item>("select 999999999999999999999999999999 decimal_value from dual").Single());
        }

        [Test]
        public void LoadStaticShouldWarn()
        {
            DbMapper.PrintWarnings = true;
            DbMapper.PrintSqls = true;
            var item = orm.Query<ItemWrongDefinition>("select id, name, decimal_value, date_value from onmm2.item where id = :0", 1).SingleOrDefault();
            DbMapper.PrintWarnings = false;
            DbMapper.PrintSqls = false;
        }

        [Test]
        public void CollectionShouldNotBeUsed()
        {
            ItemWithCollection item = new ItemWithCollection();
            item.Id = 3;
            item.GroupsNotPresentInDb = new List<int>() { 1 };
            orm.Insert(item, "onmm2.item");
        }

        [Test]
        public void ConnectAsSys()
        {
            var orm = new DbMapper();
            orm.ConnectAsSys("tstdaily", "bi");
            orm.QueryScalar<int>("select 1 from dual");
        }

        [TestCase("1\n2")]
        [TestCase("1\r\n2")]
        [TestCase("1\n\r2")]
        [TestCase("1\r2")]
        public void ClobTests(string testValue)
        {
            dynamic item = new Entity("onmm2.bigclobtest");
            item.Text = testValue;
            orm.Insert(item);

            var fromDb = orm.QueryScalar<string>("select text from onmm2.bigclobtest");
            Assert.AreEqual(testValue, fromDb);
        }

        [Test]
        public void NonQueryIgnoreErrorShouldWork()
        {
            orm.NonQueryIgnoreError("should not work");
        }

        [Test]
        public void ToEntityShouldIgnoreListProperties()
        {
            var entity = new SomeClass()
                .ToEntity();

            var dic = entity.GetDictionary();

            Assert.AreEqual(4, dic.Count);
            Assert.IsTrue(dic.ContainsKey("Prop1"));
            Assert.IsTrue(dic.ContainsKey("Prop2"));
            Assert.IsTrue(dic.ContainsKey("Prop3"));
            Assert.IsTrue(dic.ContainsKey("Prop4"));

        }
    }

    class ProcParameters
    {
        public decimal Input { get; set; }
        public decimal EchoOutput { get; set; }
        public decimal CountOutput { get; set; }
        public object FundsOutput { get; set; }
    }

    public class Item
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public decimal DecimalValue { get; set; }
        public DateTime? DateValue { get; set; }
    }

    public class ItemWithCollection
    {
        public int Id { get; set; }
        public List<int> GroupsNotPresentInDb { get; set; }
    }

    public class ItemWrongDefinition
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public decimal Decimalvalue { get; set; }
        public DateTime? Datevalue { get; set; }
    }

    class SomeClass
    {
        public string Prop1 { get; set; }
        public decimal Prop2 { get; set; }
        public DateTime Prop3 { get; set; }
        public int? Prop4 { get; set; }
        public List<string> Col1 { get; set; }
        public String[] Col2 { get; set; }
        public Dictionary<int, int> Col3 { get; set; }
    }
}
