using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis;

public partial interface IRedisClient
{
    IRedisPipeline NewPipeline() => throw new NotSupportedException();
}

public partial interface IRedisPipeline : IDisposable
{
    // StackExchange.Redis.IBatch
    T GetRaw<T>();

    IRedisPipeline AddRaw<TBatch>(Func<TBatch, Task<object>> todo) => AddRaw(out _, todo);
    IRedisPipeline AddRaw<TBatch, T>(Func<TBatch, Task<T>> todo) => AddRaw(out _, todo);

    IRedisPipeline AddRaw<TBatch>(out Task<object> task, Func<TBatch, Task<object>> todo);
    IRedisPipeline AddRaw<TBatch, T>(out Task<T> task, Func<TBatch, Task<T>> todo);

    Task<object[]> ExecuteAsync();
}

public partial interface IRedisPipeline
{
    IRedisPipeline ExistsAsync(string key);

    IRedisPipeline ExpireAsync(string key, int expSec);
    IRedisPipeline ExpireAsync(string key, TimeSpan expire);

    IRedisPipeline TtlAsync(string key);
    IRedisPipeline PttlAsync(string key);

    IRedisPipeline GetAsync(string key);
    IRedisPipeline GetAsync<T>(string key, Func<string, T> convert = null);

    /// <summary>普通删除,非模糊删除</summary>
    IRedisPipeline DeleteAsync(params string[] keys);

    IRedisPipeline SetAsync(string key, string value, int expSec = -1, bool nx = false);
    IRedisPipeline SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false);

    IRedisPipeline SetNxAsync(string key, string value, int expSec = -1) => SetAsync(key, value, expSec, true);
    IRedisPipeline SetNxAsync<T>(string key, T obj, int expSec = -1) => SetAsync(key, obj, expSec, true);

    IRedisPipeline IncrByAsync(string key, long value = 1L);
}

public partial interface IRedisPipeline
{
    IRedisPipeline HashExistsAsync(string key, string field);

    IRedisPipeline HashGetAsync(string key, string field);
    IRedisPipeline HashGetAsync<T>(string key, string field, Func<string, T> convert = null);

    IRedisPipeline HashGetAllAsync(string key);
    IRedisPipeline HashGetAllAsync<T>(string key, Func<string, T> convert = null);

    IRedisPipeline HashSetAsync(string key, string field, string value, bool nx = false);
    IRedisPipeline HashSetAsync<T>(string key, string field, T obj, bool nx = false);

    IRedisPipeline HashSetAsync(string key, IDictionary<string, string> values);
    IRedisPipeline HashSetAsync<T>(string key, IDictionary<string, T> values);

    IRedisPipeline HashDeleteAsync(string key, params string[] fields);

    IRedisPipeline HashIncrByAsync(string key, string field, long value = 1L);

    IRedisPipeline HashLengthAsync(string key);
}

public partial interface IRedisPipeline
{
    IRedisPipeline SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers);
    IRedisPipeline SortedSetAddAsync(string key, params (string, double)[] scoreMembers);

    IRedisPipeline SortedSetGetScoreAsync<T>(string key, T member);
    IRedisPipeline SortedSetGetScoreAsync(string key, string member);
    IRedisPipeline SortedSetGetRankAsync(string key, string member, bool orderByAsc = true);
    IRedisPipeline SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true);

    IRedisPipeline SortedSetCountAsync(string key);
    IRedisPipeline SortedSetCountByScoreAsync(string key, double min, double max);


    IRedisPipeline SortedSetRangeByScoreWithScores(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true);

    IRedisPipeline SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true);

    IRedisPipeline SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true);

    IRedisPipeline SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true);


    IRedisPipeline SortedSetRemoveAsync(string key, params string[] members);
    IRedisPipeline SortedSetRemoveAsync<T>(string key, params T[] members);    

    IRedisPipeline SortedSetRemoveByScoreAsync(string key, double min, double max);
    IRedisPipeline SortedSetRemoveByRankAsync(string key, long start, long stop);


    IRedisPipeline SortedSetIncrByAsync(string key, string member, double value = 1);
    IRedisPipeline SortedSetIncrByAsync<T>(string key, T member, double value = 1);
}
