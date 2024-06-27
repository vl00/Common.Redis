using CSRedis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Common
{
    public static partial class CSRedisCoreExtension
    {
        public static Task<object[]> StartPipeAsync(this CSRedisClient redis, Action<CSRedisClientPipe<string>> handler, CancellationToken cancellation = default)
        {
            using var pipe = redis.StartPipe();
            handler(pipe);
            return pipe.EndPipeAsync(cancellation);
        }

        public static Task<object[]> EndPipeAsync<T>(this CSRedisClientPipe<T> pipe, CancellationToken cancellation = default)
        {
            return Task.Factory.StartNew(o =>
            {
                using var p = (CSRedisClientPipe<T>)o;
                return p.EndPipe();
            }, pipe, cancellation);
        }

        public static Task RunOnAsync(this CSRedisClient redis, Action action) => redis.RunOnAsync(_ => action());
        public static Task RunOnAsync<T>(this CSRedisClient redis, Func<T> func) => redis.RunOnAsync(_ => func());

        public static Task RunOnAsync(this CSRedisClient redis, Action<CSRedisClient> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return Task.Factory.StartNew(o => 
            {
                var t = o as Tuple<CSRedisClient, Action<CSRedisClient>>;
                t.Item2(t.Item1);
            }, Tuple.Create(redis, action));
        }

        public static Task<T> RunOnAsync<T>(this CSRedisClient redis, Func<CSRedisClient, T> func)
        {
            if (func == null) throw new ArgumentNullException(nameof(func));
            return Task.Factory.StartNew(o => 
            {
                var t = o as Tuple<CSRedisClient, Func<CSRedisClient, T>>;
                return t.Item2(t.Item1);
            }, Tuple.Create(redis, func));
        }

        #region ScanKeys
        /**
         *   from cursor=0L to 0L and scan once time
         */

        public static IAsyncEnumerable<string> ScanKeys(this CSRedisClient redis, IEnumerable<string> keysOrPatterns, CancellationToken cancellation = default)
        {
            return ScanKeys(redis, keysOrPatterns, 1000, cancellation);
        }

        public static async IAsyncEnumerable<string> ScanKeys(this CSRedisClient redis, IEnumerable<string> keysOrPatterns, long? count, [EnumeratorCancellation] CancellationToken cancellation = default)
        {
            foreach (var keyOrPattern in keysOrPatterns)
            {
                await foreach (var key in ScanKeys(redis, keyOrPattern, count, cancellation))
                    yield return key;
            }
        }

        public static IAsyncEnumerable<string> ScanKeys(this CSRedisClient redis, string keyOrPattern, CancellationToken cancellation = default)
        {
            return ScanKeys(redis, keyOrPattern, 1000, cancellation);
        }

        public static async IAsyncEnumerable<string> ScanKeys(this CSRedisClient redis, string keyOrPattern, long? count, [EnumeratorCancellation] CancellationToken cancellation = default)
        {
            if (string.IsNullOrEmpty(keyOrPattern) || string.IsNullOrWhiteSpace(keyOrPattern))
            {
                yield break;
            }
            if (!keyOrPattern.Contains('*'))
            {
                yield return keyOrPattern;
                yield break;
            }
            var cursor = 0L;
            int i_err = 0;
            while (!cancellation.IsCancellationRequested)
            {
                string[] ks;
                try
                {
                    var scan = await redis.ScanAsync(cursor, keyOrPattern, count);
                    cursor = scan.Cursor;
                    ks = scan.Items;
                    i_err = 0;
                }
                catch  // ignore error
                {
                    if ((i_err++) >= 2) break;
                    else continue;
                }
                if (ks?.Length > 0)
                {
                    foreach (var k in ks)
                        yield return k;
                }
                if (cursor <= 0L)
                {
                    break;
                }
            }
        }

        #endregion ScanKeys
    }
}
