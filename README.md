# OdpNetMicroMapper
A convention based Oracle-only ORM. Columns in database with underscores will be translated to Camel Case. E.g. work_phone will be loaded into property WorkPhone.

            var orm = new OdpNetMicroMapper();

Fetch all columns from a table row

        	var list = orm.Query("select * from mytable");
        	Console.WriteLine("Prints all properties " + list.First());

Update a row

        	dynamic row = new Entity("mytable", "id");
        	row.Id = 1;
        	row.Name = "New Name";
        	orm.Update(row);

Delete, Insert or Merge

            orm.Delete(row);
            orm.Insert(row);
            orm.MergeInto(row);
 
Update a row with plain SQL

            orm.NonQuery("update ci.fund t set t.name = :0 where t.id = :1", "New Name", 1);

Query list of simple types

            var list = orm.QuerySingleTypeList<int>("select id from mytable").ToList();

Call a function

        	var str = orm.ExecuteFunction<string>("myfunction", 1, 2, DateTime.Now);

Call a procedure with ref cursor

        	var list = orm.ExecuteProcedure("mypropcedure", "myparametervalue",  DbMapperParameter.RefCursor);

Needed in app.config

```xml
  <appSettings>
   <add key="ConnectionString" value="data source=***;user id=***;password=***;" />
  </appSettings>

  <runtime>
     <assemblyBinding xmlns="urn:schemas-microsoft-com:asm.v1">
     <qualifyAssembly partialName="Oracle.DataAccess" fullName="Oracle.DataAccess, Version=2.102.2.20, Culture=neutral, PublicKeyToken=89b483f429c47342" />
     </assemblyBinding>
  </runtime> 
```

And much more... Please see the test project.
