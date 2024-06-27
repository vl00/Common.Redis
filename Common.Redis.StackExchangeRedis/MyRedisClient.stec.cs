using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis.StackExchangeRedis;

public partial class MyRedisClient : IRedisClient
{
    readonly ConfigurationOptions _options;
    readonly Lazy<ConnectionMultiplexer> _lazyConnection;

    public MyRedisClient(ConfigurationOptions options, int? minThreadCount = null,
        Func<object, string> funcSerialize = null, Func<string, Type, object> funcDeserialize = null)
    {
        _options = options;
        FuncSerialize = funcSerialize;
        FuncDeserialize = funcDeserialize;

        _lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            if (minThreadCount != null) ThreadPool.SetMinThreads(minThreadCount.Value, minThreadCount.Value);
            var connection = ConnectionMultiplexer.Connect(_options);
            return connection;
        });
    }

    public readonly Func<object, string> FuncSerialize;
    public readonly Func<string, Type, object> FuncDeserialize;

    public ConnectionMultiplexer GetStecConnection()
    {
        return _lazyConnection.Value;
    }

    public IDatabase GetStecDatabase()
    {
        return _lazyConnection.Value.GetDatabase();
    }

    public T GetRaw<T>()
    {
        if (typeof(T) == typeof(ConnectionMultiplexer) || typeof(T) == typeof(IConnectionMultiplexer))
        {
            var v = GetStecConnection();
            return Unsafe.As<ConnectionMultiplexer, T>(ref v);
        }
        if (typeof(T) == typeof(IDatabase))
        //if (typeof(T) == typeof(IDatabase) || typeof(T) == typeof(RedisDatabase))
        {
            var v = GetStecDatabase();
            return Unsafe.As<IDatabase, T>(ref v);
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
        var db = GetStecDatabase();
        return db.KeyExistsAsync(key);
    }

    public Task<bool> ExpireAsync(string key, int expSec)
    {
        var db = GetStecDatabase();
        return ExpireAsync(db, key, TimeSpan.FromSeconds(expSec));
    }

    public Task<bool> ExpireAsync(string key, TimeSpan expire)
    {
        var db = GetStecDatabase();
        return ExpireAsync(db, key, expire);
    }

    static Task<bool> ExpireAsync(IDatabase db, string key, TimeSpan expire)
    {
        return db.KeyExpireAsync(key, expire);
    }

    public async Task<long> TtlAsync(string key)
    {
        var db = GetStecDatabase();
        var k = new RedisKey(key);
        var t = await db.ExecuteAsync("TTL", k);
        return (long)t;
    }

    public async Task<long> PttlAsync(string key)
    {
        var db = GetStecDatabase();
        var k = new RedisKey(key);
        // db.KeyTimeToLiveAsync() -2和-1都返回null
        var t = await db.ExecuteAsync("PTTL", k);
        return (long)t;
    }

    public async Task<string> GetAsync(string key)
    {
        var db = GetStecDatabase();
        var s = await db.StringGetAsync(key);
        return s;
    }

    public async Task<T> GetAsync<T>(string key)
    {
        var s = await GetAsync(key);
        return OnDeserialize<T>(s);
    }

    public Task<long> DeleteAsync(params string[] keys)
    {
        var db = GetStecDatabase();
        return db.KeyDeleteAsync(keys.Select(_ => (RedisKey)_).ToArray());
    }

    public async Task<bool> SetAsync(string key, string value, int expSec = -1, bool nx = false)
    {
        var db = GetStecDatabase();
        var b = await db.StringSetAsync(key, value, TimeSpan.FromSeconds(expSec), nx ? When.NotExists : When.Always, CommandFlags.None); // internal impl call del when 'value==null'
        return b;
    }

    public Task<bool> SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false)
    {
        var s = OnSerialize(obj);
        return SetAsync(key, s, expSec, nx);
    }

    public async Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, TimeSpan exp, bool nx = false)
    {
        var db = GetStecDatabase();
        var rr = await db.StringGetAsync(key);
        if (rr.HasValue)
        {
            return OnDeserialize<T>(rr);
        }
        var v = await func();
        if (v != null)
        {
            var s = OnSerialize(v);
            await db.StringSetAsync(key, s, exp, nx ? When.NotExists : When.Always, CommandFlags.None);
        }
        return v;
    }

    public Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, int expSec, bool nx = false)
    {
        return GetOrSetAsync(key, func, TimeSpan.FromSeconds(expSec), nx);
    }

    public Task<long> IncrByAsync(string key, long value = 1L)
    {
        var db = GetStecDatabase();
        return db.StringIncrementAsync(key, value);
    }
}

public partial class MyRedisClient
{
    public Task<bool> HashExistsAsync(string key, string field)
    {
        var db = GetStecDatabase();
        return db.HashExistsAsync(key, field);
    }

    public async Task<string> HashGetAsync(string key, string field)
    {
        var db = GetStecDatabase();
        var s = await db.HashGetAsync(key, field);
        return s;
    }

    public async Task<T> HashGetAsync<T>(string key, string field)
    {
        var s = await HashGetAsync(key, field);
        return OnDeserialize<T>(s);
    }

    public async Task<Dictionary<string, string>> HashGetAllAsync(string key)
    {
        var db = GetStecDatabase();
        var r = await db.HashGetAllAsync(key);
        return r.ToDictionary(_ => (string)_.Name, _ => (string)_.Value);
    }

    public async Task<Dictionary<string, T>> HashGetAllAsync<T>(string key)
    {
        var db = GetStecDatabase();
        var r = await db.HashGetAllAsync(key);
        return r.ToDictionary(_ => (string)_.Name, _ => OnDeserialize<T>((string)_.Value));
    }

    public async Task<bool> HashSetAsync(string key, string field, string value, TimeSpan? expire = null, bool nx = false)
    {
        var db = GetStecDatabase();
        var b = await db.HashSetAsync(key, field, value, nx ? When.NotExists : When.Always); // internal impl call hdel when 'value==null'
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
        var db = GetStecDatabase();
        await HashSetAsync(db, key, values.Select(_ => new HashEntry(_.Key, _.Value)).ToArray(), expire);
    }

    public async Task HashSetAsync<T>(string key, IDictionary<string, T> values, TimeSpan? expire = null)
    {
        var db = GetStecDatabase();
        await HashSetAsync(db, key, values.Select(_ => new HashEntry(_.Key, OnSerialize(_.Value))).ToArray(), expire);
    }

    static async Task HashSetAsync(IDatabase db, string key, HashEntry[] entries, TimeSpan? expire = null)
    {
        await db.HashSetAsync(key, entries);
        if (expire != null && expire != Timeout.InfiniteTimeSpan)
        {
            await ExpireAsync(db, key, expire.Value);
        }
    }

    public Task<long> HashDeleteAsync(string key, params string[] fields)
    {
        var db = GetStecDatabase();
        return db.HashDeleteAsync(key, fields.Select(_ => new RedisValue(_)).ToArray());
    }

    public Task<long> HashIncrByAsync(string key, string field, long value = 1L)
    {
        var db = GetStecDatabase();
        return db.HashIncrementAsync(key, field, value);
    }

    public Task<long> HashLengthAsync(string key)
    {
        var db = GetStecDatabase();
        return db.HashLengthAsync(key);
    }
}

public partial class MyRedisClient
{
    public async Task<long> SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (scoreMembers?.Any() != true) return 0L;
        var db = GetStecDatabase();
        var i = await db.SortedSetAddAsync(key, scoreMembers.Select(s => new SortedSetEntry(OnSerialize(s.Item1), s.Item2)).ToArray(), CommandFlags.None);
        return i;
    }

    public async Task<long> SortedSetAddAsync(string key, params (string, double)[] scoreMembers)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (scoreMembers?.Any() != true) return 0L;
        var db = GetStecDatabase();
        var i = await db.SortedSetAddAsync(key, scoreMembers.Select(s => new SortedSetEntry(s.Item1, s.Item2)).ToArray(), CommandFlags.None);
        return i;
    }

    public async Task<double?> SortedSetGetScoreAsync<T>(string key, T member)
    {
        var db = GetStecDatabase();
        var m = OnSerialize(member);
        var s = await db.SortedSetScoreAsync(key, m);
        return s;
    }

    public async Task<double?> SortedSetGetScoreAsync(string key, string member)
    {
        var db = GetStecDatabase();
        var s = await db.SortedSetScoreAsync(key, member);
        return s;
    }

    public async Task<long?> SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true)
    {
        var db = GetStecDatabase();
        var m = OnSerialize(member);
        var i = await db.SortedSetRankAsync(key, m, orderByAsc ? Order.Ascending : Order.Descending);
        return i;
    }

    public async Task<long?> SortedSetGetRankAsync(string key, string member, bool orderByAsc = true)
    {
        var db = GetStecDatabase();            
        var i = await db.SortedSetRankAsync(key, member, orderByAsc ? Order.Ascending : Order.Descending);
        return i;
    }

    public async Task<long> SortedSetCountAsync(string key)
    {
        var db = GetStecDatabase();
        var c = await db.SortedSetLengthAsync(key);
        return c;
    }

    public async Task<long> SortedSetCountByScoreAsync(string key, double min, double max)
    {
        var db = GetStecDatabase();
        var c = await db.SortedSetLengthAsync(key, min, max);
        return c;
    }

    public async Task<Dictionary<string, double>> SortedSetRangeByScoreWithScores(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var db = GetStecDatabase();
        var v = await db.SortedSetRangeByScoreWithScoresAsync(key, min, max, skip: offset, take: (count ?? -1L), order: (orderByAsc ? Order.Ascending : Order.Descending));
        return v.ToDictionary(_ => (string)_.Element, _ => _.Score);
    }

    public async Task<Dictionary<T, double>> SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var db = GetStecDatabase();
        var v = await db.SortedSetRangeByScoreWithScoresAsync(key, min, max, skip: offset, take: (count ?? -1L), order: (orderByAsc ? Order.Ascending : Order.Descending));
        return v.ToDictionary(_ => OnDeserialize<T>((string)_.Element), _ => _.Score);
    }

    public async Task<Dictionary<string, double>> SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        var db = GetStecDatabase();
        var v = await db.SortedSetRangeByRankWithScoresAsync(key, start, stop, order: (orderByAsc ? Order.Ascending : Order.Descending));
        return v.ToDictionary(_ => (string)_.Element, _ => _.Score);
    }

    public async Task<Dictionary<T, double>> SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        var db = GetStecDatabase();
        var v = await db.SortedSetRangeByRankWithScoresAsync(key, start, stop, order: (orderByAsc ? Order.Ascending : Order.Descending));
        return v.ToDictionary(_ => OnDeserialize<T>((string)_.Element), _ => _.Score);
    }

    public async Task<long> SortedSetRemoveAsync<T>(string key, params T[] members)
    {
        if (members == null || !members.Any()) return 0L;
        var db = GetStecDatabase();
        var i = await db.SortedSetRemoveAsync(key, members.Select(o => new RedisValue(OnSerialize(o))).ToArray());
        return i;
    }

    public async Task<long> SortedSetRemoveAsync(string key, params string[] members)
    {
        if (members == null || !members.Any()) return 0L;
        var db = GetStecDatabase();
        var i = await db.SortedSetRemoveAsync(key, members.Select(o => new RedisValue(o)).ToArray());
        return i;
    }

    public async Task<long> SortedSetRemoveByScoreAsync(string key, double min, double max)
    {
        var db = GetStecDatabase();
        var i = await db.SortedSetRemoveRangeByScoreAsync(key, min, max);
        return i;
    }

    public async Task<long> SortedSetRemoveByRankAsync(string key, long start, long stop)
    {
        var db = GetStecDatabase();
        var i = await db.SortedSetRemoveRangeByRankAsync(key, start, stop);
        return i;
    }

    public async Task<double> SortedSetIncrByAsync<T>(string key, T member, double value = 1)
    {
        var db = GetStecDatabase();
        var m = OnSerialize(member);
        var s = await db.SortedSetIncrementAsync(key, m, value);
        return s;
    }

    public async Task<double> SortedSetIncrByAsync(string key, string member, double value = 1)
    {
        var db = GetStecDatabase();
        var s = await db.SortedSetIncrementAsync(key, member, value);
        return s;
    }
}

public partial class MyRedisClient
{
    public async Task<object> EvalLuaAsync(string script, string key, params object[] args)
    {
        var db = GetStecDatabase();
        var v = await db.ScriptEvaluateAsync(script, new[] { new RedisKey(key) }, args.Select(a => (RedisValue.Unbox(a))).ToArray());
        return v;
    }

    public async Task<object> EvalLuaAsync(string script, string[] keys, object[] args)
    {
        var db = GetStecDatabase();
        var v = await db.ScriptEvaluateAsync(script, keys.Select(k => new RedisKey(k)).ToArray(), args.Select(a => (RedisValue.Unbox(a))).ToArray()); //!! (RedisValue)a 会报错
        return v;
    }
}
