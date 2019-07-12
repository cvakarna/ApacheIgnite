using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Client.Cache;
using Apache.Ignite.Linq;
using LinkQExample.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LinkQExample
{
    class Program
    {
        private  IgniteClientConfiguration _igniteClientConfiguration;
        public Program(string endPoint)
        {
            _igniteClientConfiguration = new IgniteClientConfiguration
            {
                Endpoints = new string[] { endPoint },
                SocketTimeout = TimeSpan.FromSeconds(30)

            };
        }

      
        static void Main(string[] args)
        {
            Program p = new Program("localhost");
            //p.PopulateMessages();
            //p.QueryLinq();
            p.PopulateEmployees();
            p.CompiledQueryExample();

            Console.ReadKey();
        }

        static  Func<IQueryable<ICacheEntry<string, ChatMessege>>,string,IQueryCursor<ICacheEntry<string,ChatMessege>>> compile = CompiledQuery.Compile((IQueryable<ICacheEntry<string, ChatMessege>> query, string name) => query.Where(msg => msg.Value.Name == name));
        public void QueryLinq()
       {
            try
            {
                using (IIgniteClient client = Ignition.StartClient(this._igniteClientConfiguration))
                {
                    //get cache configuraation
                    var cache = client.GetCache<string, ChatMessege>("messages");
                    //var data = cache.Query(new SqlFieldsQuery("select * from ChatMessege  Where Name = 'abc'")).GetAll();
                    //Console.WriteLine(JsonConvert.SerializeObject(data));
                    //IQueryable<ICacheEntry<string, ChatMessege>> msgs = cache.AsCacheQueryable().OrderByDescending(msg => msg.Value.CreatedDateTime).Where(msg => msg.Value.Name == "abc").Take(2);
                    var cache0 = cache.AsCacheQueryable();
                    //Compiled Query
                    //var compileQuery = CompiledQuery.Compile((IQueryable<ICacheEntry<string,ChatMessege>> query,string name) => query.Where(msg => msg.Value.Name == name));
                   // var compileQuery = CompiledQuery.Compile((string name) => cache0.Where(msg => msg.Value.Name == name));


                    //IQueryable<ICacheEntry<string, ChatMessege>> qry = (from msg in msgs orderby msg.Value.CreatedDateTime descending select msg).Take(2);
                    foreach (ICacheEntry<string, ChatMessege> entry in compile.Invoke(cache0,"abc"))
                        Console.WriteLine(">>>     " + entry.Value.CreatedDateTime + "\t" + entry.Value.Name);

                }
            }
            catch(Exception ex)
            {

            }
         
        }
        public void PopulateMessages()
        {
            List<ChatMessege> messages = new List<ChatMessege>
            {
                new ChatMessege{Name="abc",CreatedDateTime="1562770732"},
                new ChatMessege{Name="abc",CreatedDateTime="1562770728"},
                new ChatMessege{Name="abc-1",CreatedDateTime="1562770724"},
                new ChatMessege{Name="abc-3",CreatedDateTime="1562770742"},
                new ChatMessege{Name="abc-3",CreatedDateTime="1562770743"}


            };

            foreach (var mess in messages){

                using (IIgniteClient client = Ignition.StartClient(this._igniteClientConfiguration))
                {
                    //get cache configuraation
                    var cache = client.GetOrCreateCache<string, ChatMessege>(GetOrCreaeMessageCacheConfig("messages"));
                       
                   
                     cache.Put(mess.Name, mess);
                    Console.WriteLine(cache.Get(mess.Name).Name); 
                }
            }

        }

        public  CacheClientConfiguration GetOrCreaeMessageCacheConfig(string cacheName)
        {
            var cacheConfig = new CacheClientConfiguration
            {

                CacheMode = Apache.Ignite.Core.Cache.Configuration.CacheMode.Partitioned,
                Name = cacheName,
                QueryEntities = new[]
                {
                     new QueryEntity(typeof(string), typeof(ChatMessege))
                }
            };
            return cacheConfig;
        }


        public CacheClientConfiguration GetOrCreateEmployeeCacheConfig(string cacheName)
        {
            var cacheConfig = new CacheClientConfiguration
            {

                CacheMode = Apache.Ignite.Core.Cache.Configuration.CacheMode.Partitioned,
                Name = cacheName,
                QueryEntities = new[]
                {
                     new QueryEntity(typeof(string), typeof(Employee))
                }
            };
            return cacheConfig;
        }
        /// <summary>
        /// Queries employees that have specific salary with a compiled query.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private  void CompiledQueryExample()
        {
            using (IIgniteClient client = Ignition.StartClient(this._igniteClientConfiguration))
            {

                var cache = client.GetCache<string, Employee>("Employees");
                const int minSalary = 200;
                var queryable = cache.AsCacheQueryable();

                //this query causing issue
                Func<string, IQueryCursor<ICacheEntry<string, Employee>>> issueqry =
                    CompiledQuery.Compile((string empName) => queryable.Where(emp => emp.Value.Name == empName));

                //with this no issue
                Func<int, IQueryCursor<ICacheEntry<string, Employee>>> qry =
                    CompiledQuery.Compile((int min) => queryable.Where(emp => emp.Value.Salary == min));

                foreach (var entry in qry(minSalary))
                    Console.WriteLine(">>>    " + entry.Value.Name);

                foreach (var entry in issueqry("abc"))
                    Console.WriteLine(">>>    " + entry.Value.Name);

            }
        }


        public void PopulateEmployees()
        {
            List<Employee> messages = new List<Employee>
            {
                  new Employee{Name="abc",Salary=100,EmpId="1"},
                  new Employee{Name="abc-1",Salary=200,EmpId="2"},
                  new Employee{Name="abc-2",Salary=400,EmpId="3"},
                  new Employee{Name="abc-3",Salary=300,EmpId="4"},
                  new Employee{Name="abc-4",Salary=200,EmpId="5"},


            };
            ICacheClient<string, Employee> cache = null;
            foreach (var mess in messages)
            {

                using (IIgniteClient client = Ignition.StartClient(this._igniteClientConfiguration))
                {
                    //get cache configuraation
                     cache = client.GetOrCreateCache<string, Employee>(GetOrCreateEmployeeCacheConfig("Employees"));


                    cache.Put(mess.EmpId, mess);
                    Console.WriteLine(cache.Get(mess.EmpId).Name);

                }
            }
          

        }
    }
}
