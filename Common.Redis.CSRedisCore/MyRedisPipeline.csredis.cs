using CSRedis;
using CSRedis.Internal.ObjectPool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis.CSRedisCore;

public partial class MyRedisClient : IRedisClient
{
    public IRedisPipeline NewPipeline() => new MyRedisPipeline(this);
}

public partial class MyRedisPipeline
{
    readonly MyRedisClient _my;
    readonly CSRedisClient _redis;
    List<(Action<CSRedisClientPipe<string>>, Func<object, object>)> _tasks; // (f,convert)[]
    CSRedisClientPipe<string> _pipe;
    TaskCompletionSource<object[]> _endPipeTask;

    public MyRedisPipeline(MyRedisClient redis)
    {
        _my = redis;
        _redis = redis.GetCSRedisClient();
        Ensure(ref _endPipeTask);
    }

    public T GetRaw<T>()
    {
        if (typeof(T) == typeof(CSRedisClientPipe<string>))
        {
            Ensure(ref _pipe);
            return AsTo<CSRedisClientPipe<string>, T>(_pipe);
        }
        throw new NotSupportedException();
    }

    void Ensure(ref CSRedisClientPipe<string> pipe) => pipe ??= _redis.StartPipe();
    void Ensure(ref TaskCompletionSource<object[]> task) => task ??= new TaskCompletionSource<object[]>();

    static T2 AsTo<T1, T2>(T1 t1) => Unsafe.As<T1, T2>(ref t1);

    public IRedisPipeline AddRaw<TBatch>(Func<TBatch, Task<object>> todo)
    {
        PrivateAddRaw(todo, out _, false, null);
        return this;
    }

    public IRedisPipeline AddRaw<TBatch, T>(Func<TBatch, Task<T>> todo)
    {
        PrivateAddRaw(todo, out _, false, null);
        return this;
    }

    public IRedisPipeline AddRaw<TBatch>(out Task<object> task, Func<TBatch, Task<object>> todo, Func<object, object> convert = null)
    {
        PrivateAddRaw(todo, out task, true, convert);
        return this;
    }

    public IRedisPipeline AddRaw<TBatch, T>(out Task<T> task, Func<TBatch, Task<T>> todo, Func<object, T> convert = null)
    {
        PrivateAddRaw(todo, out task, true, convert);
        return this;
    }

    // csredis的pipeline api只有EndPipe方法才会有返回值,
    // 所以这里的todo返回的Task可以无所谓
    void PrivateAddRaw<TBatch, T>(Func<TBatch, Task<T>> todo, out Task<T> task, bool isNeedOut, Func<object, T> convert)
    {
        if (typeof(TBatch) != typeof(CSRedisClientPipe<string>)) throw new ArgumentException($"type 'TBatch' is not match");

        Action<CSRedisClientPipe<string>> f = pipe => todo(AsTo<CSRedisClientPipe<string>, TBatch>(pipe));
        if (isNeedOut)
        {
            if (convert != null) AddTodo(f, (r) => convert(r));
            else AddTodo(f);
            task = GetTask<T>(_tasks, f);
        }
        else
        {
            AddTodo(f);
            task = null;
        }
    }

    async Task<T> GetTask<T>(List<(Action<CSRedisClientPipe<string>>, Func<object, object>)> ls, Action<CSRedisClientPipe<string>> f)
    {
        var t = _endPipeTask!.Task;
        var ret = await t;
        var i = ls.FindIndex(x => x.Item1 == f);
        if (i == -1) throw new InvalidOperationException("Not found, may be removed");
        var r = ret[i];
        if (ls[i].Item2 != null) r = ls[i].Item2(r);
        return (T)r;
    }

    protected void AddTodo(Action<CSRedisClientPipe<string>> f, Func<object, object> convert = null)
    {
        _tasks ??= new List<(Action<CSRedisClientPipe<string>>, Func<object, object>)>();
        _tasks.Add((f, convert));
    }

    public async Task<object[]> ExecuteAsync()
    {
        var tasks = _tasks;
        _tasks = null;
        try
        {
            if (tasks == null || tasks.Count == 0) return Array.Empty<object>();

            var t = _endPipeTask.Task;
            await Task.Factory.StartNew(() => Complete(tasks));
            var r = await t.ConfigureAwait(false);
            return r;
        }
        finally
        {
            _pipe = null;
        }
    }

    void Complete(List<(Action<CSRedisClientPipe<string>>, Func<object, object>)> tasks)
    {
        try
        {
            if (tasks != null)
            {
                Ensure(ref _pipe);

                foreach (var (f, _) in tasks)
                {
                    f(_pipe); // 网络不稳, csredis的pipeline可能会报错
                }
            }
            var r = _pipe!.EndPipe();
            _endPipeTask.TrySetResult(r);
        }
        catch (Exception ex)
        {
            _endPipeTask.TrySetException(ex);
        }
        finally
        {
            _endPipeTask = null;
            if (tasks != null)
            {
                Ensure(ref _endPipeTask);
            }
            else
            {
                // no need '_pipe.Dispose()' is '_pipe.EndPipe()'
                //_pipe.Dispose();
            }
            _pipe = null;
        }
    }

    public void Dispose()
    {
        if (_pipe != null) Complete(null);
        _tasks = null;
    }

    protected T DoConvert<T>(Func<string, T> convert, string value)
    {
        return convert != null ? convert(value) : _my.OnDeserialize<T>(value);
    }
}

public partial class MyRedisPipeline : IRedisPipeline
{
    public IRedisPipeline ExistsAsync(string key)
    {
        AddTodo(pipe => pipe.Exists(key));
        return this;
    }

    public IRedisPipeline ExpireAsync(string key, int expSec)
    {
        AddTodo(pipe => pipe.Expire(key, expSec));
        return this;
    }

    public IRedisPipeline ExpireAsync(string key, TimeSpan expire)
    {
        AddTodo(pipe => pipe.Expire(key, expire));
        return this;
    }

    public IRedisPipeline TtlAsync(string key)
    {
        AddTodo(pipe => pipe.Ttl(key));
        return this;
    }

    public IRedisPipeline PttlAsync(string key)
    {
        AddTodo(pipe => pipe.PTtl(key));
        return this;
    }

    public IRedisPipeline GetAsync(string key)
    {
        AddTodo(pipe => pipe.Get(key));
        return this;
    }

    public IRedisPipeline GetAsync<T>(string key, Func<string, T> convert = null)
    {
        AddTodo(pipe => pipe.Get(key), r => DoConvert(convert, r?.ToString()));
        return this;
    }

    public virtual IRedisPipeline DeleteAsync(params string[] keys)
    {
        if (keys?.Length != 1) throw new NotSupportedException("api限制不支持");
        var key = keys[0];
        if (_my.EnableUnLink) AddTodo(pipe => pipe.UnLink(key));
        else AddTodo(pipe => pipe.Del(key));
        return this;
    }

    public virtual IRedisPipeline SetAsync(string key, string value, int expSec = -1, bool nx = false)
    {
        if (value == null)
        {
            if (_my.EnableUnLink) AddTodo(pipe => pipe.UnLink(key), r => Convert.ToInt64(r) > 0);
            else AddTodo(pipe => pipe.Del(key), r => Convert.ToInt64(r) > 0);
        }
        else
        {
            if (nx) AddTodo(pipe => pipe.Set(key, value, expSec, RedisExistence.Nx));
            else AddTodo(pipe => pipe.Set(key, value, expSec));
        }
        return this;
    }

    public IRedisPipeline SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false)
    {
        var s = _my.OnSerialize(obj);
        return SetAsync(key, s, expSec, nx);
    }

    public IRedisPipeline IncrByAsync(string key, long value = 1L)
    {
        AddTodo(pipe => pipe.IncrBy(key, value));
        return this;
    }
}

public partial class MyRedisPipeline
{
    public IRedisPipeline HashExistsAsync(string key, string field)
    {
        AddTodo(pipe => pipe.HExists(key, field));
        return this;
    }

    public IRedisPipeline HashGetAsync(string key, string field)
    {
        AddTodo(pipe => pipe.HGet(key, field));
        return this;
    }

    public IRedisPipeline HashGetAsync<T>(string key, string field, Func<string, T> convert = null)
    {
        AddTodo(pipe => pipe.HGet(key, field), r => DoConvert(convert, r?.ToString()));
        return this;
    }

    public IRedisPipeline HashGetAllAsync(string key)
    {
        AddTodo(pipe => pipe.HGetAll(key));
        return this;
    }

    public IRedisPipeline HashGetAllAsync<T>(string key, Func<string, T> convert = null)
    {
        AddTodo(pipe => pipe.HGetAll(key), o => 
        {
            var r = (Dictionary<string, string>)o;
            return r.ToDictionary(_ => _.Key, _ => DoConvert(convert, _.Value));
        });
        return this;
    }

    public virtual IRedisPipeline HashSetAsync(string key, string field, string value, bool nx = false)
    {
        if (value == null)
        {
            AddTodo(pipe => pipe.HDel(key, field), (r) => Convert.ToInt64(r) > 0);
        }
        else
        {
            AddTodo(pipe => pipe.HSet(key, field, value));
        }
        return this;
    }

    public IRedisPipeline HashSetAsync<T>(string key, string field, T obj, bool nx = false)
    {
        var s = _my.OnSerialize(obj);
        return HashSetAsync(key, field, s, nx);
    }

    public IRedisPipeline HashSetAsync(string key, IDictionary<string, string> values)
    {
        var len = values?.Count ?? 0;
        if (len == 0) throw new ArgumentException("not be null or empty");
        var arr = new object[len * 2];
        var i = 0;
        foreach (var (f, v) in values)
        {
            arr[i++] = f;
            arr[i++] = v;
        }

        AddTodo(pipe => pipe.HMSet(key, arr));
        return this;
    }

    public IRedisPipeline HashSetAsync<T>(string key, IDictionary<string, T> values)
    {
        var len = values?.Count ?? 0;
        if (len == 0) throw new ArgumentException("not be null or empty");
        var arr = new object[len * 2];
        var i = 0;
        foreach (var (f, v) in values)
        {
            arr[i++] = f;
            arr[i++] = _my.OnSerialize(v);
        }

        AddTodo(pipe => pipe.HMSet(key, arr));
        return this;
    }

    public IRedisPipeline HashDeleteAsync(string key, params string[] fields)
    {
        AddTodo(pipe => pipe.HDel(key, fields));
        return this;
    }

    public IRedisPipeline HashIncrByAsync(string key, string field, long value = 1L)
    {
        AddTodo(pipe => pipe.HIncrBy(key, field, value));
        return this;
    }

    public IRedisPipeline HashLengthAsync(string key)
    {
        AddTodo(pipe => pipe.HLen(key));
        return this;
    }
}

public partial class MyRedisPipeline
{
    public IRedisPipeline SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers)
    {
        AddTodo(pipe => pipe.ZAdd(key, scoreMembers.Select(s => (Convert.ToDecimal(s.Item2), (object)s.Item1)).ToArray()));
        return this;
    }

    public IRedisPipeline SortedSetAddAsync(string key, params (string, double)[] scoreMembers)
    {
        AddTodo(pipe => pipe.ZAdd(key, scoreMembers.Select(s => (Convert.ToDecimal(s.Item2), (object)s.Item1)).ToArray()));
        return this;
    }

    public IRedisPipeline SortedSetGetScoreAsync<T>(string key, T member)
    {
        AddTodo(pipe => pipe.ZScore(key, member), r => r == null ? (double?)null : Convert.ToDouble(r));
        return this;
    }

    public IRedisPipeline SortedSetGetScoreAsync(string key, string member)
    {
        AddTodo(pipe => pipe.ZScore(key, member), r => r == null ? (double?)null : Convert.ToDouble(r));
        return this;
    }

    public IRedisPipeline SortedSetGetRankAsync(string key, string member, bool orderByAsc = true)
    {
        if (orderByAsc) AddTodo(pipe => pipe.ZRank(key, member));
        else AddTodo(pipe => pipe.ZRevRank(key, member));
        return this;
    }

    public IRedisPipeline SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true)
    {
        if (orderByAsc) AddTodo(pipe => pipe.ZRank(key, member));
        else AddTodo(pipe => pipe.ZRevRank(key, member));
        return this;
    }

    public IRedisPipeline SortedSetCountAsync(string key)
    {
        AddTodo(pipe => pipe.ZCard(key));
        return this;
    }

    public IRedisPipeline SortedSetCountByScoreAsync(string key, double min, double max)
    {
        var s1 = MyRedisClient.GetScoreString(ref min);
        var s2 = MyRedisClient.GetScoreString(ref max);
        AddTodo(pipe => pipe.ZCount(key, s1, s2));
        return this;
    }

    public virtual IRedisPipeline SortedSetRangeByScoreWithScores(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var s1 = MyRedisClient.GetScoreString(ref min);
        var s2 = MyRedisClient.GetScoreString(ref max);
        var c = count ?? -1L; // 有bug
        if (orderByAsc) AddTodo(pipe => pipe.ZRangeByScoreWithScores(key, s1, s2, offset: offset, count: c), r => (((string, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        else AddTodo(pipe => pipe.ZRevRangeByScoreWithScores(key, s1, s2, offset: offset, count: c), r => (((string, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        return this;
    }

    public virtual IRedisPipeline SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true)
    {
        var s1 = MyRedisClient.GetScoreString(ref min);
        var s2 = MyRedisClient.GetScoreString(ref max);
        var c = count ?? -1L; // 有bug
        if (orderByAsc) AddTodo(pipe => pipe.ZRangeByScoreWithScores<T>(key, s1, s2, offset: offset, count: c), r => (((T, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        else AddTodo(pipe => pipe.ZRevRangeByScoreWithScores<T>(key, s1, s2, offset: offset, count: c), r => (((T, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        return this;
    }

    public IRedisPipeline SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        if (orderByAsc) AddTodo(pipe => pipe.ZRangeWithScores(key, start, stop), r => (((string, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        else AddTodo(pipe => pipe.ZRevRangeWithScores(key, start, stop), r => (((string, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        return this;
    }

    public IRedisPipeline SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true)
    {
        if (orderByAsc) AddTodo(pipe => pipe.ZRangeWithScores<T>(key, start, stop), r => (((T, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        else AddTodo(pipe => pipe.ZRevRangeWithScores<T>(key, start, stop), r => (((T, decimal)[])r).ToDictionary(_ => _.Item1, _ => Convert.ToDouble(_.Item2)));
        return this;
    }

    public IRedisPipeline SortedSetRemoveAsync(string key, params string[] members)
    {
        AddTodo(pipe => pipe.ZRem(key, members));
        return this;
    }

    public IRedisPipeline SortedSetRemoveAsync<T>(string key, params T[] members)
    {
        AddTodo(pipe => pipe.ZRem(key, members));
        return this;
    }

    public IRedisPipeline SortedSetRemoveByScoreAsync(string key, double min, double max)
    {
        var s1 = MyRedisClient.GetScoreString(ref min);
        var s2 = MyRedisClient.GetScoreString(ref max);
        AddTodo(pipe => pipe.ZRemRangeByScore(key, s1, s2));
        return this;
    }

    public IRedisPipeline SortedSetRemoveByRankAsync(string key, long start, long stop)
    {
        AddTodo(pipe => pipe.ZRemRangeByRank(key, start, stop));
        return this;
    }

    public IRedisPipeline SortedSetIncrByAsync(string key, string member, double value = 1)
    {
        AddTodo(pipe => pipe.ZIncrBy(key, member, Convert.ToDecimal(value)));
        return this;
    }

    public IRedisPipeline SortedSetIncrByAsync<T>(string key, T member, double value = 1)
    {
        var m = _my.OnSerialize(member);
        AddTodo(pipe => pipe.ZIncrBy(key, m, Convert.ToDecimal(value)));
        return this;
    }
}
