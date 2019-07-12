using Apache.Ignite.Core.Cache.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace LinkQExample.Model
{
    public class ChatMessege
    {
        [QuerySqlField]
        public string Name { get; set; }
        [QuerySqlField]
        public string CreatedDateTime { get; set; }
    }
}
