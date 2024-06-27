using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis
{
    public static partial class RedisClientExtension
    {
        // -- local ttlva = redis.call('pttl', KEYS[1])
        public const string LuaExtendLock = 
@"local lckid = redis.call('GET', KEYS[1])
if lckid == ARGV[1] then
  local ttlva = 0
  redis.call('pexpire', KEYS[1], ARGV[2] + ttlva)
  return 1
end
return 0";

        public const string LuaUnlock = 
@"local lckid = redis.call('get', KEYS[1])
if lckid == ARGV[1] then
  redis.call('del', KEYS[1])
  return 1
end
return 0";

        /// <summary>
        /// try get lock
        /// </summary>
        public static Task<string> LockExAcquireAsync(this IRedisClient redis, string ck, int expMs = 30000,
            int retry = 2, int perRetryDelayMs = 1000, bool perRetryDelayIsRandom = true, bool throwLastError = false)
        {            
            return LockExAcquireAsync(redis, ck, null, expMs, retry, perRetryDelayMs, perRetryDelayIsRandom, throwLastError);
        }

        /// <summary>
        /// try get lock
        /// </summary>
        /// <param name="redis"></param>
        /// <param name="ck"></param>
        /// <param name="lckid"></param>
        /// <param name="expMs">锁过期毫秒s</param>
        /// <param name="retry">
        /// 重试次数.<br/>
        /// 0=不重试<br/>
        /// -1=无限重试<br/>
        /// </param>
        /// <param name="perRetryDelayMs">每次重试的时间毫秒s</param>
        /// <param name="perRetryDelayIsRandom">每次重试的时间是否随机</param>
        /// <param name="throwLastError">是否throw最后一次错误异常</param>
        /// <returns></returns>
        public static async Task<string> LockExAcquireAsync(this IRedisClient redis, string ck, string lckid, int expMs = 30000, 
            int retry = 2, int perRetryDelayMs = 1000, bool perRetryDelayIsRandom = true, bool throwLastError = false)
        {
            lckid ??= Guid.NewGuid().ToString("n");
            Random rnd = null;
            for (int i = 0, c = retry; ;)
            {
                Exception ex = null;
                try
                {
                    if (await redis.SetAsync(ck, lckid, expMs / 1000, true).ConfigureAwait(false))
                    {
                        return lckid; // lck ok
                    }
                }
                catch (Exception ex0)
                {
                    ex = ex0;
                }
                if ((++i) <= c || c == -1) // 即将进行第n次重试前准备
                {
                    var ms = perRetryDelayIsRandom ? (rnd ??= new Random()).Next(0, perRetryDelayMs + 1) : perRetryDelayMs;
                    await Task.Delay(ms).ConfigureAwait(false);
                }
                else
                {
                    if (ex != null && throwLastError) throw ex; // lck fail and throw error
                    break;
                }
            }
            return null; // lck fail
        }

        /// <summary>
        /// 延期短锁
        /// </summary>
        public static async Task<bool> LockExExtendAsync(this IRedisClient redis, string ck, string lckid, int expMs = 30000)
        {
            var b = await redis.EvalLuaAsync(LuaExtendLock, ck, lckid, expMs).ConfigureAwait(false);
            return b?.ToString() == "1";
        }

        /// <summary>
        /// unlock短锁. 只能unlock相同的锁id
        /// </summary>
        public static async Task<bool> LockExReleaseAsync(this IRedisClient redis, string ck, string lckid)
        {
            var b = await redis.EvalLuaAsync(LuaUnlock, ck, lckid).ConfigureAwait(false);
            return b?.ToString() == "1";
        }
    }
}
