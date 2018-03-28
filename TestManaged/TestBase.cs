using System;
using NUnit.Framework;
using OdpNetMicroMapper;

namespace TestManaged
{
    [TestFixture]
    public class TestBase
    {
        static bool firstRun = true;
        protected DbMapper orm = new DbMapper();
        protected DbMapper sysOrm = new DbMapper();

        private void CreateUser()
        {
            if (!firstRun)
                return;

            firstRun = false;

            sysOrm.NonQueryIgnoreError("drop user onmm2 cascade");
            sysOrm.NonQuery("create user onmm2 identified by onmm2");
            sysOrm.NonQuery("grant create session to onmm2");
            sysOrm.NonQuery("alter user onmm2 quota unlimited on users");
            Console.WriteLine("user onmm2 created");

            var sql = "create table onmm2.bigclobtest (text clob)";
            sysOrm.NonQuery(sql);

            sql = "create table onmm2.rawtest (bytes raw(16))";
            sysOrm.NonQuery(sql);

            sql = "create table onmm2.item_with_long (text long)";
            sysOrm.NonQuery(sql);

            sql = @"create table onmm2.item_odd (id number(10) not null, yield_2date number)";
            sysOrm.NonQuery(sql);

            sql = @"create table onmm2.item_composite_key (id  number(10) not null, type  number(10) not null, text varchar2(100))";
            sysOrm.NonQuery(sql);

            sql = @"create table onmm2.item (id number(10) not null, name varchar2(100), decimal_value number, date_value timestamp default sysdate not null)";
            sysOrm.NonQuery(sql);

            sql = @"create sequence onmm2.seq_items start with 3";
            sysOrm.NonQuery(sql);

            sql = @"create or replace function onmm2.append1function (pString varchar2) return varchar2
                as
                begin
                    return pString ||  '1';
                end;";
            //todo fix elsewhere? //http://boncode.blogspot.com/2009/03/oracle-pls-00103-encountered-symbol.html
            sql = sql.Replace(Environment.NewLine, "\n");
            sysOrm.NonQuery(sql);

            sql = @"create or replace function onmm2.plus1function (pNumber number) return integer
                    as
                    begin
                         return pNumber + 1;
                    end;";
            sql = sql.Replace(Environment.NewLine, "\n");
            sysOrm.NonQuery(sql);

            sql = @"create or replace procedure onmm2.get_items_by_name(pName in varchar2, io_cursor out SYS_REFCURSOR) is
                    begin
                        open io_cursor for
                         select * from onmm2.item t where t.name = pName;
                    end;";
            sql = sql.Replace(Environment.NewLine, "\n");
            sysOrm.NonQuery(sql);

            sql = @"create or replace procedure onmm2.rename_item(pId number, pName in varchar2) is
                    begin
                        update onmm2.item t set t.name = pName where t.id = pId;
                    end;";
            sql = sql.Replace(Environment.NewLine, "\n");
            sysOrm.NonQuery(sql);

            sql = @"create or replace procedure onmm2.proc_with_many_out_parameters(pInput number, pEcho out number, pCount out number, io_cursor out SYS_REFCURSOR) is
                    begin
                        select pInput into pEcho from dual;
                        select count(1) into pCount from onmm2.fund;
                        open io_cursor for
                            select * from onmm2.fund;
                    end;";
            sql = sql.Replace(Environment.NewLine, "\n");
            sysOrm.NonQuery(sql);
        }
           
        [TearDown]
        public void DropSchema()
        {
        }

        [SetUp]
        public void CreateSchema()
        {
            //throw new Exception("Please set 'data source' and 'password' in function CreateSchema()");

            sysOrm.ConnectAsSys("INTSHARE", "bi");
            orm.ConnectionString = "data source=INTSHARE;user id=onmm2;password=onmm2;";

            CreateUser();

            sysOrm.NonQuery("delete from onmm2.item_composite_key");
            sysOrm.NonQuery("delete from onmm2.item");
            sysOrm.NonQuery("delete from onmm2.bigclobtest");
            sysOrm.NonQuery("delete from onmm2.item_with_long");
            sysOrm.NonQuery("delete from onmm2.item_odd");
            sysOrm.NonQuery("delete from onmm2.rawtest");

            var sql = "insert into onmm2.item_odd (id, yield_2date) values (1, 99)";
            sysOrm.NonQuery(sql);

            sql = "insert into onmm2.item (id, name, decimal_value) values (1, 'First Item', 0.321)";
            sysOrm.NonQuery(sql);

            sql = "insert into onmm2.item (id, name, decimal_value) values (2, 'Second Item', 0.123)";
            sysOrm.NonQuery(sql);

        }
    }
}
