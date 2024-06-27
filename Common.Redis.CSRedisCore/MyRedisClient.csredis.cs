using CSRedis;
using CSRedis.Internal.ObjectPool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis.CSRedisCore;

public partial class MyRedisClient : IRedisClient
{
    readonly CSRedisClient _redis;

    public MyRedisClient(CSRedisClient redisClient,
        Func<object, string> funcSerialize = null, Func<string, Type, object> funcDeserialize = null)
    {
        _redis = redisClient;            

        if (funcSerialize != null) _redis.CurrentSerialize = funcSerialize;
        if (funcDeserialize != null) _redis.CurrentDeserialize = funcDeserialize;
        FuncSerialize = _redis.CurrentSerialize;
        FuncDeserialize = _redis.CurrentDeserialize;
    }

    public bool EnableUnLink = true;

    public readonly Func<object, string> FuncSerialize;
    public readonly Func<string, Type, object> FuncDeserialize;

    public CSRedisClient GetCSRedisClient() => _redis;

    public T GetRaw<T>()
    {
        if (typeof(T) == typeof(CSRedisClient))
        {
            var v = GetCSRedisClient();
            return Unsafe.As<CSRedisClient, T>(ref v);
        }
        throw new NotSupportedException();
    }

    public Task<T> GetRawAsync<T>() => Task.FromResult(GetRaw<T>());

    public string OnSerialize<T>(T o)
    {
        return typeof(T) == typeof(string) ? o?.ToString() : FuncSerialize(o);
    }

    public T OnDeserialize<T>(string s)
    {
        if (typeof(T) == typeof(string)) return Unsafe.As<string, T>(ref s);
        return (T)FuncDeserialize(s, typeof(T));
    }
}

public partial class MyRedisClient
{ 
    public Task<bool> ExistsAsync(string key)
    {
        var db = GetCSRedisClient();
        return db.ExistsAsync(key);
    }

    public Task<bool> ExpireAsync(string key, int expSec)
    {
        var db = GetCSRedisClient();
        return db.ExpireAsync(key, expSec);
    }

    public Task<bool> ExpireAsync(string key, TimeSpan expire)
    {
        var db = GetCSRedisClient();
        return ExpireAsync(db, key, expire);
    }

    static Task<bool> ExpireAsync(CSRedisClient db, string key, TimeSpan expire)
    {
        return db.PExpireAsync(key, (int)expire.TotalMilliseconds);
    }

    public async Task<long> TtlAsync(string key)
    {
        var db = GetCSRedisClient();
        var t = await db.TtlAsync(key);
        return t;
    }

    public async Task<long> PttlAsync(string key)
    {
        var db = GetCSRedisClient();
        var t = await db.PTtlAsync(key);
        return t;
    }

    public Task<string> GetAsync(string key)
    {
        var db = GetCSRedisClient();
        return db.GetAsync(key);
    }

    public Task<T> GetAsync<T>(string key)
    {
        var db = GetCSRedisClient();
        return db.GetAsync<T>(key);
    }

    public Task<long> DeleteAsync(params string[] keys)
    {
        var db = GetCSRedisClient();
        return DeleteAsync(db, keys);
    }

    async Task<long> DeleteAsync(CSRedisClient db, params string[] keys)
    {
        var i = 0L;
        if (EnableUnLink) i = await db.UnLinkAsync(keys);
        else i = await db.DelAsync(keys);
        return i;
    }

    public async Task<bool> SetAsync(string key, string value, int expSec = -1, bool nx = false)
    {
        var db = GetCSRedisClient();
        if (value == null)
        {
            var i = await DeleteAsync(db, key);
            return i > 0;
        }
        var t = nx ? db.SetAsync(key, value, expSec, RedisExistence.Nx) : db.SetAsync(key, value, expSec);
        return await t;
    }

    public async Task<bool> SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false)
    {
        var db = GetCSRedisClient();
        if (Equals(obj, null))
        {
            var i = await DeleteAsync(db, key);
            return i > 0;
        }
        var t = nx ? db.SetAsync(key, obj, expSec, RedisExistence.Nx) : db.SetAsync(key, obj, expSec);
        return await t;
    }

    public async Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, TimeSpan exp, bool nx = false)
    {
        var db = GetCSRedisClient();
        var rr = await db.GetAsync(key);
        if (rr != null)
        {
            return OnDeserialize<T>(rr);
        }
        var v = await func();
        if (v != null)
        {
            var s = OnSerialize(v);
            if (nx) await db.SetAsync(key, s, exp, RedisExistence.Nx);
            else await db.SetAsync(key, s, exp);
        }
        return v;
    }

    public Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, int expSec, bool nx = false)
    {
        return GetOrSetAsync(key, func, TimeSpan.FromSeconds(expSec), nx);
    }

    public Task<long> IncrByAsync(string key, long value = 1L)
    {
        var db = GetCSRedisClient();
        return db.IncrByAsync(key, value);
    } 
}

public partial class MyRedisClient
{
    public Task<bool> HashExistsAsync(string key, string field)
    {
        var db = GetCSRedisClient();
        return db.HExistsAsync(key, field);
    }

    public Task<string> HashGetAsync(string key, string field)
    {
        var db = GetCSRedisClient();
        return db.HGetAsync(key, field);
    }

    public Task<T> HashGetAsync<T>(string key, string field)
    {
        var db = GetCSRedisClient();
        return db.HGetAsync<T>(key, field);
    }

    public Task<Dictionary<string, string>> HashGetAllAsync(string key)
    {
        var db = GetCSRedisClient();
        return db.HGetAllAsync(key);
    }

    public Task<Dictionary<string, T>> HashGetAllAsync<T>(string key)
    {
        var db = GetCSRedisClient();
        return db.HGetAllAsync<T>(key);
    }

    public async Task<bool> HashSetAsync(string key, string field, string value, TimeSpan? expire = null, bool nx = false)
    {
        var db = GetCSRedisClient();
        if (value == null)
        {
            var i = await db.HDelAsync(key, field);
            return i > 0;
        }
        var t = nx ? db.HSetNxAsync(key, field, value) : db.HSetAsync(key, field, value);
        var b = await t;
        if (expire != null && expire != Timeout.InfiniteTimeSpan)
        {
            await ExpireAsync(db, key, expire.Value);
        }
        return b;
    }

    public Task<bool> HashSetAsync<T>(string key, string field, T obj, TimeSpan? expire = null, bool nx = false)
    {
        var s = OnSerialize(obj);
        return HashSetAsync(key, field, s, expire, nx);
    }

    public async Task HashSetAsync(string key, IDictionary<string, string> values, TimeSpan? expire = null)
    {
        var len = values?.Count ?? 0;
        if (len == 0) return;
        var arr = new object[len * 2];
        var i = 0;
        foreach (var (f, v) in values)
        {
            arr[i++] = f;
            arr[i++] = v;
        }

        var db = GetCSRedisClient();
        await HMSetAsync(db, key, arr, expire);
    }

    public async Task HashSetAsync<T>(string key, IDictionary<string, T> values, TimeSpan? expire = null)
    {
        var len = values?.Count ?? 0;
        if (len == 0) return;
        var arr = new object[len * 2];
        var i = 0;
        foreach (var (f, v) in values)
        {
            arr[i++] = f;
            arr[i++] = OnSerialize(v);
        }

        var db = GetCSRedisClient();
        await HMSetAsync(db, key, arr, expire);
    }

    static async Task HMSetAsync(CSRedisClient db, string key, object[] entries, TimeSpan? expire = null)
    {
        await db.HMSetAsync(key, entries);
        if (expire != null && expire != Timeout.InfiniteTimeSpan)
        {
            await ExpireAsync(db, key, expire.Value);
        }
    }

    public Task<long> HashDeleteAsync(string key, params string[] fields)
    {
        var db = GetCSRedisClient();
        return db.HDelAsync(key, fields);
    }

    public Task<long> HashIncrByAsync(string key, string field, long value = 1L)
    {
        var db = GetCSRedisClient();
        return db.HIncrByAsync(key, field, value);
    }

    public Task<long> HashLengthAsync(string key)
    {
        var db = GetCSRedisClient();
        return db.HLenAsync(key);
    }
}

public partial class MyRedisClient
{
    public async Task<long> SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers)
    {
        var db = GetCSRedisClient();
        var i = await db.ZAddAsync(key, scoreMembers.Select(s => (Convert.ToDecimal(s.Item2), (object)s.Item1)).ToArray());
        return i;
    }

    public async Task<long> SortedSetAddAsync(string key, params (string, double)[] scoreMembers)
    {
        var db = GetCSRedisClient();
        var i = await db.ZAddAsync(key, scoreMembers.Select(s => (Convert.ToDecimal(s.Item2), (object)s.Item1)).ToArray());
        return i;
    }

    public async Task<double?> SortedSetGetScoreAsync<T>(string key, T member)
    {
        var db = GetCSRedisClient();            
        var s = await db.ZScoreAsync(key, member);
        return s == null ? (double?)null : Convert.ToDouble(s);
    }

    public async Task<double?> SortedSetGetScoreAsync(string key, string member)
    {
        var db = GetCSRedisClient();
        var s = await db.ZScoreAsync(key, member);
        return s == null ? (double?)null : Convert.ToDouble(s);
    }

    public async Task<long?> SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var i = orderByAsc ? await db.ZRankAsync(key, member)
            : await db.ZRevRankAsync(key, member);
        return i;
    }

    public async Task<long?> SortedSetGetRankAsync(string key, string member, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var i = orderByAsc ? await db.ZRankAsync(key, member)
            : await db.ZRevRankAsync(key, member);
        return i;
    }

    public async Task<long> SortedSetCountAsync(string key)
    {
        var db = GetCSRedisClient();
        var c = await db.ZCardAsync(key);
        return c;
    }

    static string GetScoreString(ref double score)
    {
        return double.IsNegativeInfinity(score) ? "-inf"
            : double.IsPositiveInfinity(score) ? "+inf" 
            : score.ToString();
    }

    public async Task<long> SortedSetCountByScoreAsync(string key, double min, double max)
    {
        var db = GetCSRedisClient();
        var s1 = GetScoreString(ref min);
        var s2 = GetScoreString(ref max);
        var c = await db.ZCountAsync(key, s1, s2);
        return c;
    }

    public async Task<Dictionary<string, double>> SortedSetRangeByScoreWithScores(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var s1 = GetScoreString(ref min);
        var s2 = GetScoreString(ref max);
        var v = orderByAsc ? await db.ZRangeByScoreWithScoresAsync(key, s1, s2, offset: offset, count: count)
            : await db.ZRevRangeByScoreWithScoresAsync(key, s2, s1, offset: offset, count: count);
        return v.ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2));
    }

    public async Task<Dictionary<T, double>> SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var s1 = GetScoreString(ref min);
        var s2 = GetScoreString(ref max);
        var v = orderByAsc ? await db.ZRangeByScoreWithScoresAsync<T>(key, s1, s2, offset: offset, count: count)
            : await db.ZRevRangeByScoreWithScoresAsync<T>(key, s2, s1, offset: offset, count: count);
        return v.ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2));
    }

    public async Task<Dictionary<string, double>> SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var v = orderByAsc ? await db.ZRangeWithScoresAsync(key, start, stop)
            : await db.ZRevRangeWithScoresAsync(key, start, stop);
        return v.ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2));
    }

    public async Task<Dictionary<T, double>> SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        var db = GetCSRedisClient();
        var v = orderByAsc ? await db.ZRangeWithScoresAsync<T>(key, start, stop)
            : await db.ZRevRangeWithScoresAsync<T>(key, start, stop);
        return v.ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2));
    }

    public async Task<long> SortedSetRemoveAsync<T>(string key, params T[] members)
    {
        if (members == null || !members.Any()) return 0L;
        var db = GetCSRedisClient();
        var i = await db.ZRemAsync(key, members);
        return i;
    }

    public async Task<long> SortedSetRemoveAsync(string key, params string[] members)
    {
        if (members == null || !members.Any()) return 0L;
        var db = GetCSRedisClient();
        var i = await db.ZRemAsync(key, members);
        return i;
    }

    public async Task<long> SortedSetRemoveByScoreAsync(string key, double min, double max)
    {
        var db = GetCSRedisClient();
        var s1 = GetScoreString(ref min);
        var s2 = GetScoreString(ref max);
        var i = await db.ZRemRangeByScoreAsync(key, s1, s2);
        return i;
    }

    public async Task<long> SortedSetRemoveByRankAsync(string key, long start, long stop)
    {
        var db = GetCSRedisClient();
        var i = await db.ZRemRangeByRankAsync(key, start, stop);
        return i;
    }

    public async Task<double> SortedSetIncrByAsync<T>(string key, T member, double value = 1)
    {
        var db = GetCSRedisClient();
        var m = OnSerialize(member);
        var s = await db.ZIncrByAsync(key, m, Convert.ToDecimal(value));
        return Convert.ToDouble(s);
    }

    public async Task<double> SortedSetIncrByAsync(string key, string member, double value = 1)
    {
        var db = GetCSRedisClient();            
        var s = await db.ZIncrByAsync(key, member, Convert.ToDecimal(value));
        return Convert.ToDouble(s);
    }
}

public partial class MyRedisClient
{
    const int SHA1Length = 40;
    static readonly Regex regexSha1 = new Regex("^[0-9a-f]{40}$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    readonly HashSet<string> luaSha1s = new HashSet<string>();

    // see StackExchange.Redis source code
    internal static bool IsSHA1(string script) => script != null && script.Length == SHA1Length && regexSha1.IsMatch(script);

    public async Task<object> EvalLuaAsync(string script, string key, params object[] args)
    {
        var db = GetCSRedisClient();
        if (IsSHA1(script))
        {
            var v = await db.EvalSHAAsync(script, key, args);
            return v;
        }
        else
        {
            var sha1 = GetSha1(script);
            
            var isContained = false;
            lock (luaSha1s) isContained = !luaSha1s.Add(sha1);
            if (isContained)
            {
                try
                {
                    var v = await db.EvalSHAAsync(sha1, key, args);
                    return v;
                }
                catch (Exception ex)
                {
                    if (IsNoMatchSha1(ex)) lock (luaSha1s) luaSha1s.Remove(sha1);
                    else throw;
                }
            }
            try { await db.ScriptLoadAsync(script).ConfigureAwait(false); }
            catch { lock (luaSha1s) luaSha1s.Remove(sha1); }
            // finally
            {
                var v = await db.EvalAsync(script, key, args).ConfigureAwait(false);
                return v;
            }
        }
    }

    public async Task<object> EvalLuaAsync(string script, string[] keys, object[] args)
    {
        if (keys == null || keys.Length == 0) throw new ArgumentException("keys is null or empty", nameof(keys));
        if (keys.Length == 1) return await EvalLuaAsync(script, keys[0], args);
        
        var db = GetCSRedisClient();
        var isSHA1 = IsSHA1(script);
        var strLua = script;

        if (!isSHA1)
        {
            var sha1 = GetSha1(script);
            var isContained = false;
            lock (luaSha1s) isContained = !luaSha1s.Add(sha1);
            if (isContained)
            {
                strLua = sha1;
                isSHA1 = true;
            }
            else
            {
                try { await db.ScriptLoadAsync(script).ConfigureAwait(false); }
                catch { lock (luaSha1s) luaSha1s.Remove(sha1); }
            }
        }

        var mi1 = typeof(CSRedisClient).GetMethod("SerializeRedisValueInternal", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
        var mi2 = typeof(CSRedisClient).GetMethod("ExecuteArrayAsync", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).MakeGenericMethod(typeof(object));
        //
        // object[] args2 = args?.Select((object z) => SerializeRedisValueInternal(z)).ToArray();
        var args2 = args?.Select(z => mi1.Invoke(db, new object[] { z })).ToArray();
        //
        // Task<T[]> ExecuteArrayAsync<T>(string[] key, Func<Object<RedisClient>, string[], Task<T[]>> handerAsync)
        Func<Object<RedisClient>, string[], Task<object[]>> handerAsync = async (c, ks) => 
        {
            var reload = false;
            try 
            { 
                if (isSHA1) 
                    return new object[] { await c.Value.EvalSHAAsync(strLua, ks, args2).ConfigureAwait(false) }; 
            } 
            catch
            { 
                if (strLua == script) throw;
                lock (luaSha1s) luaSha1s.Remove(strLua);
                reload = true;
            }
            if (reload)
            {
                try { await db.ScriptLoadAsync(script).ConfigureAwait(false); } 
                catch { } // ignore error
            }
            return new object[] { await c.Value.EvalAsync(script, ks, args2).ConfigureAwait(false) };
        };
        //
        var v = await (mi2.Invoke(db, new object[] { keys, handerAsync }) as Task<object[]>);
        return v[0];
    }

    static bool IsNoMatchSha1(Exception ex)
    {
        return ex.Message?.Contains("NOSCRIPT") == true || ex.Message?.Contains("No matching") == true;
    }

    static string GetSha1(string s)
    {
        using var ha = SHA1.Create();
        var str = Encoding.UTF8.GetBytes(s);
        var bys = ha.ComputeHash(str);
        var sb = new StringBuilder();
        foreach (var b in bys)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }
}
