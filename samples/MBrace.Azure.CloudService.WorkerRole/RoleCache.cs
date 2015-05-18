using MBrace.Store.Internals;
using Microsoft.ApplicationServer.Caching;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Azure.CloudService.WorkerRole
{
    class RoleCache : IObjectCache
    {
        static Lazy<DataCache> cache = new Lazy<DataCache>(() => new DataCacheFactory().GetDefaultCache());

        public Microsoft.FSharp.Control.FSharpAsync<bool> Add(string key, object value)
        {
            try
            {
                var item = cache.Value.Add(key, value);
                return Microsoft.FSharp.Control.FSharpAsync.AwaitTask(Task.FromResult(true));
            }
            catch(DataCacheException)
            {
                return Microsoft.FSharp.Control.FSharpAsync.AwaitTask(Task.FromResult(false));
            }
        }

        public Microsoft.FSharp.Control.FSharpAsync<bool> ContainsKey(string key)
        {
            return Microsoft.FSharp.Control.FSharpAsync.AwaitTask(Task.FromResult(cache.Value.GetCacheItem(key) != null));
        }

        public Microsoft.FSharp.Control.FSharpAsync<Microsoft.FSharp.Core.FSharpOption<object>> TryFind(string key)
        {
            var item = cache.Value.Get(key);
            var result = item == null ? Microsoft.FSharp.Core.FSharpOption<object>.None : Microsoft.FSharp.Core.FSharpOption<object>.Some(item);
            return Microsoft.FSharp.Control.FSharpAsync.AwaitTask(Task.FromResult(result));
        }
    }
}
