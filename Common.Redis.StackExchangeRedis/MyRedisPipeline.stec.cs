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
    public IRedisPipeline NewPipeline() => new MyRedisPipeline(this);
}

public partial class MyRedisPipeline : IRedisPipeline
{
    List<Task<object>> _tasks;    
    readonly IBatch _batch;
    readonly MyRedisClient _redis;

    public MyRedisPipeline(MyRedisClient redis)
    {
        _redis = redis;
        _batch = redis.GetRaw<IDatabase>().CreateBatch();
    }

    public T GetRaw<T>()
    {
        if (typeof(T) == typeof(IBatch))
        {
            var v = _batch;
            return Unsafe.As<IBatch, T>(ref v);
        }
        throw new NotSupportedException();
    }

    public IRedisPipeline AddRaw<TBatch>(Func<TBatch, Task<object>> todo) => AddRaw(out _, todo);
    public IRedisPipeline AddRaw<TBatch, T>(Func<TBatch, Task<T>> todo) => AddRaw(out _, todo);

    public IRedisPipeline AddRaw<TBatch>(out Task<object> task, Func<TBatch, Task<object>> todo, Func<object, object> convert = null)
    {
        if (_batch is not TBatch b) throw new ArgumentException($"type 'TBatch' is not match");
        // 其实对于StackExchange.Redis, convert参数可以ignore
        if (convert == null) AddTodo(out task, b, todo);
        else AddTodo(out task, b, (b) => todo(b).ContinueWith(t => convert(t.Result), TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline AddRaw<TBatch, T>(out Task<T> task, Func<TBatch, Task<T>> todo, Func<object, T> convert = null)
    {
        if (_batch is not TBatch b) throw new ArgumentException($"type 'TBatch' is not match");
        // 其实对于StackExchange.Redis, convert参数可以ignore
        if (convert == null) AddTodo(out task, b, todo);
        else AddTodo(out task, b, (b) => todo(b).ContinueWith(t => convert(t.Result), TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    protected void AddTodo<TBatch>(out Task<object> t, TBatch _batch, Func<TBatch, Task<object>> todo)
    {
        _tasks ??= new List<Task<object>>();
        _tasks.Add(t = todo?.Invoke(_batch));
    }

    protected void AddTodo<TBatch, T>(out Task<T> t, TBatch _batch, Func<TBatch, Task<T>> todo)
    {
        _tasks ??= new List<Task<object>>();
        _tasks.Add(Wrap(t = todo?.Invoke(_batch)));
    }

    static async Task<object> Wrap<T>(Task<T> task)
    {
        var r = await task;
        return r;
    }

    public async Task<object[]> ExecuteAsync()
    {
        var tasks = _tasks;
        _tasks = null;
        _batch.Execute();
        if (tasks != null) await Task.WhenAll(tasks).ConfigureAwait(false);
        return tasks?.Select(_ => _?.Result)?.ToArray() ?? Array.Empty<object>();
    }

    public void Dispose()
    {
        (_batch as IDisposable)?.Dispose();
    }

    protected T DoConvert<T>(Func<string, T> convert, string value)
    {
        return convert != null ? convert(value) : _redis.OnDeserialize<T>(value);
    }
}

public partial class MyRedisPipeline
{
    public IRedisPipeline ExistsAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.KeyExistsAsync(key));
        return this;
    }

    public IRedisPipeline ExpireAsync(string key, int expSec) => ExpireAsync(key, TimeSpan.FromSeconds(expSec));

    public IRedisPipeline ExpireAsync(string key, TimeSpan expire)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.KeyExpireAsync(key, expire));
        return this;
    }

    public IRedisPipeline TtlAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.ExecuteAsync("TTL", key).ContinueWith(t => (long)t.Result, TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline PttlAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.ExecuteAsync("PTTL", key).ContinueWith(t => (long)t.Result, TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline GetAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.StringGetAsync(key).ContinueWith(t => (string)t.Result, TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline GetAsync<T>(string key, Func<string, T> convert = null)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.StringGetAsync(key).ContinueWith(t => DoConvert(convert, t.Result), TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline DeleteAsync(params string[] keys)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.KeyDeleteAsync(keys.Select(_ => (RedisKey)_).ToArray()));
        return this;
    }

    public IRedisPipeline SetAsync(string key, string value, int expSec = -1, bool nx = false)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.StringSetAsync(key, value, TimeSpan.FromSeconds(expSec), nx ? When.NotExists : When.Always, CommandFlags.None));
        return this;
    }

    public IRedisPipeline SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false)
    {
        var s = _redis.OnSerialize(obj);
        return SetAsync(key, s, expSec, nx);
    }

    public IRedisPipeline IncrByAsync(string key, long value = 1L)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.StringIncrementAsync(key, value));
        return this;
    }
}

public partial class MyRedisPipeline
{
    public IRedisPipeline HashExistsAsync(string key, string field)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashExistsAsync(key, field));
        return this;
    }

    public IRedisPipeline HashGetAsync(string key, string field)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashGetAsync(key, field).ContinueWith(t => (string)t.Result, TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline HashGetAsync<T>(string key, string field, Func<string, T> convert = null)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashGetAsync(key, field).ContinueWith(t => DoConvert(convert, t.Result), TaskContinuationOptions.OnlyOnRanToCompletion));
        return this;
    }

    public IRedisPipeline HashGetAllAsync(string key)
    {
        AddTodo(out _, _batch, async (IBatch batch) => 
        {
            var r = await batch.HashGetAllAsync(key);
            return r.ToDictionary(_ => (string)_.Name, _ => (string)_.Value);
        });
        return this;
    }

    public IRedisPipeline HashGetAllAsync<T>(string key, Func<string, T> convert = null)
    {
        AddTodo(out _, _batch, async (IBatch batch) =>
        {
            var r = await batch.HashGetAllAsync(key);
            return r.ToDictionary(_ => (string)_.Name, _ => DoConvert(convert, _.Value));
        });
        return this;
    }

    public IRedisPipeline HashSetAsync(string key, string field, string value, bool nx = false)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashSetAsync(key, field, value, nx ? When.NotExists : When.Always));
        return this;
    }

    public IRedisPipeline HashSetAsync<T>(string key, string field, T obj, bool nx = false)
    {
        var s = _redis.OnSerialize(obj);
        return HashSetAsync(key, field, s, nx);
    }

    public IRedisPipeline HashSetAsync(string key, IDictionary<string, string> values)
    {
        AddTodo(out _, _batch, async (IBatch batch) =>
        {
            await batch.HashSetAsync(key, values.Select(_ => new HashEntry(_.Key, _.Value)).ToArray());
            return null;
        });
        return this;
    }

    public IRedisPipeline HashSetAsync<T>(string key, IDictionary<string, T> values)
    {
        AddTodo(out _, _batch, async (IBatch batch) =>
        {
            await batch.HashSetAsync(key, values.Select(_ => new HashEntry(_.Key, _redis.OnSerialize(_.Value))).ToArray());
            return null;
        });
        return this;
    }

    public IRedisPipeline HashDeleteAsync(string key, params string[] fields)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashDeleteAsync(key, fields.Select(_ => new RedisValue(_)).ToArray()));
        return this;
    }

    public IRedisPipeline HashIncrByAsync(string key, string field, long value = 1L)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashIncrementAsync(key, field, value));
        return this;
    }

    public IRedisPipeline HashLengthAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.HashLengthAsync(key));
        return this;
    }
}

public partial class MyRedisPipeline
{
    public IRedisPipeline SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            if (scoreMembers?.Any() != true) return 0L;
            var i = await db.SortedSetAddAsync(key, scoreMembers.Select(s => new SortedSetEntry(_redis.OnSerialize(s.Item1), s.Item2)).ToArray(), CommandFlags.None);
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetAddAsync(string key, params (string, double)[] scoreMembers)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            if (scoreMembers?.Any() != true) return 0L;
            var i = await db.SortedSetAddAsync(key, scoreMembers.Select(s => new SortedSetEntry(s.Item1, s.Item2)).ToArray(), CommandFlags.None);
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetGetScoreAsync<T>(string key, T member)
    {
        var m = _redis.OnSerialize(member);
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetScoreAsync(key, m));
        return this;
    }

    public IRedisPipeline SortedSetGetScoreAsync(string key, string member)
    {        
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetScoreAsync(key, member));
        return this;
    }

    public IRedisPipeline SortedSetGetRankAsync(string key, string member, bool orderByAsc = true)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetRankAsync(key, member, orderByAsc ? Order.Ascending : Order.Descending));
        return this;
    }

    public IRedisPipeline SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true)
    {
        var m = _redis.OnSerialize(member);
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetRankAsync(key, m, orderByAsc ? Order.Ascending : Order.Descending));
        return this;
    }

    public IRedisPipeline SortedSetCountAsync(string key)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetLengthAsync(key));
        return this;
    }

    public IRedisPipeline SortedSetCountByScoreAsync(string key, double min, double max)
    {
        AddTodo(out _, _batch, (IBatch batch) => batch.SortedSetLengthAsync(key, min, max));
        return this;
    }

    public IRedisPipeline SortedSetRangeByScoreWithScores(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var v = await db.SortedSetRangeByScoreWithScoresAsync(key, min, max, skip: offset, take: (count ?? -1L), order: (orderByAsc ? Order.Ascending : Order.Descending));
            return v.ToDictionary(_ => (string)_.Element, _ => _.Score);
        });
        return this;
    }

    public IRedisPipeline SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var v = await db.SortedSetRangeByScoreWithScoresAsync(key, min, max, skip: offset, take: (count ?? -1L), order: (orderByAsc ? Order.Ascending : Order.Descending));
            return v.ToDictionary(_ => _redis.OnDeserialize<T>((string)_.Element), _ => _.Score);
        });
        return this;
    }

    public IRedisPipeline SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var v = await db.SortedSetRangeByRankWithScoresAsync(key, start, stop, order: (orderByAsc ? Order.Ascending : Order.Descending));
            return v.ToDictionary(_ => (string)_.Element, _ => _.Score);
        });
        return this;
    }

    public IRedisPipeline SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var v = await db.SortedSetRangeByRankWithScoresAsync(key, start, stop, order: (orderByAsc ? Order.Ascending : Order.Descending));
            return v.ToDictionary(_ => _redis.OnDeserialize<T>((string)_.Element), _ => _.Score);
        });
        return this;
    }

    public IRedisPipeline SortedSetRemoveAsync(string key, params string[] members)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            if (members == null || !members.Any()) return 0L;
            var i = await db.SortedSetRemoveAsync(key, members.Select(o => new RedisValue(o)).ToArray());
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetRemoveAsync<T>(string key, params T[] members)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            if (members == null || !members.Any()) return 0L;
            var i = await db.SortedSetRemoveAsync(key, members.Select(o => new RedisValue(_redis.OnSerialize(o))).ToArray());
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetRemoveByScoreAsync(string key, double min, double max)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var i = await db.SortedSetRemoveRangeByScoreAsync(key, min, max);
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetRemoveByRankAsync(string key, long start, long stop)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var i = await db.SortedSetRemoveRangeByRankAsync(key, start, stop);
            return i;
        });
        return this;
    }

    public IRedisPipeline SortedSetIncrByAsync(string key, string member, double value = 1)
    {
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var s = await db.SortedSetIncrementAsync(key, member, value);
            return s;
        });
        return this;
    }

    public IRedisPipeline SortedSetIncrByAsync<T>(string key, T member, double value = 1)
    {
        var m = _redis.OnSerialize(member);
        AddTodo(out _, _batch, async (IBatch db) =>
        {
            var s = await db.SortedSetIncrementAsync(key, m, value);
            return s;
        });
        return this;
    }
}
