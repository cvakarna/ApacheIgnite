using Apache.Ignite.Core.Cache.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace LinkQExample.Model
{
    public class Employee
    {

        [QuerySqlField]
        public string Name { get; set; }

        /// <summary>
        /// Salary.
        /// </summary>
        [QuerySqlField]
        public long Salary { get; set; }

        [QuerySqlField]
        public string  EmpId { get; set; }



    }
}
