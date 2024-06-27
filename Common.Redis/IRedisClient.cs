using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis;

[BusinessCode]
public partial interface IRedisClient
{
    //CSRedis.CSRedisClient GetCSRedisClient();
    //StackExchange.Redis.ConnectionMultiplexer GetStecConnection();
    //StackExchange.Redis.IDatabase GetStecDatabase();
    T GetRaw<T>();
    Task<T> GetRawAsync<T>();

    // string OnSerialize<T>(T o);
    // T OnDeserialize<T>(string s);
}

public partial interface IRedisClient
{ 
    Task<bool> ExistsAsync(string key);

    Task<bool> ExpireAsync(string key, int expSec);
    Task<bool> ExpireAsync(string key, TimeSpan expire);

    Task<long> TtlAsync(string key);
    Task<long> PttlAsync(string key);

    Task<T> GetAsync<T>(string key);
    Task<string> GetAsync(string key);

    /// <summary>普通删除,非模糊删除</summary>
    Task<long> DeleteAsync(params string[] keys);

    Task<bool> SetAsync(string key, string value, int expSec = -1, bool nx = false);
    Task<bool> SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false);

    Task<bool> SetNxAsync(string key, string value, int expSec = -1) => SetAsync(key, value, expSec, true);
    Task<bool> SetNxAsync<T>(string key, T obj, int expSec = -1) => SetAsync(key, obj, expSec, true);

    /// <summary>
    /// func(async()=>{...}) 可返回默认对象值防止返回null时多次查数据库
    /// </summary>
    Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, TimeSpan exp, bool nx = false);
    /// <inheritdoc cref="GetOrSetAsync"/>
    Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> func, int expSec, bool nx = false);

    Task<long> IncrByAsync(string key, long value = 1L);
}

public partial interface IRedisClient
{
    Task<bool> HashExistsAsync(string key, string field);

    Task<string> HashGetAsync(string key, string field);
    Task<T> HashGetAsync<T>(string key, string field);

    Task<Dictionary<string, string>> HashGetAllAsync(string key);
    Task<Dictionary<string, T>> HashGetAllAsync<T>(string key);

    Task<bool> HashSetAsync(string key, string field, string value, TimeSpan? expire = null, bool nx = false);
    Task<bool> HashSetAsync<T>(string key, string field, T obj, TimeSpan? expire = null, bool nx = false);

    Task HashSetAsync(string key, IDictionary<string, string> values, TimeSpan? expire = null);
    Task HashSetAsync<T>(string key, IDictionary<string, T> values, TimeSpan? expire = null);

    Task<long> HashDeleteAsync(string key, params string[] fields);

    Task<long> HashIncrByAsync(string key, string field, long value = 1L);

    Task<long> HashLengthAsync(string key);
}

public partial interface IRedisClient
{
    /// <summary>
    /// zadd 向有序集合添加一个或多个成员，或者更新已存在成员的分数
    /// </summary>
    Task<long> SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers);
    /// <inheritdoc cref="SortedSetAddAsync"/>
    Task<long> SortedSetAddAsync(string key, params (string, double)[] scoreMembers);

    /// <summary>
    /// zscore 返回有序集中,成员的分数值
    /// </summary>
    Task<double?> SortedSetGetScoreAsync<T>(string key, T member);
    /// <inheritdoc cref="SortedSetGetScoreAsync"/>
    Task<double?> SortedSetGetScoreAsync(string key, string member);

    /// <summary>
    /// zrank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。<br/>
    /// zrevrank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从大到小)顺序排列。<br/>
    /// </summary>
    Task<long?> SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true);
    /// <inheritdoc cref="SortedSetGetRankAsync"/>
    Task<long?> SortedSetGetRankAsync(string key, string member, bool orderByAsc = true);

    /// <summary>
    /// zcard 获取有序集合的成员数量
    /// </summary>
    Task<long> SortedSetCountAsync(string key);
    /// <summary>
    /// zcount 用于计算有序集合中指定分数区间的成员数量
    /// </summary>
    Task<long> SortedSetCountByScoreAsync(string key, double min, double max);


    /// <summary>
    /// 返回有序集中指定分数（score）区间内的所有的成员
    /// <br/>zrangebyscore key min max [WITHSCORES] [LIMIT offset count]
    /// <br/>zrevrangebyscore key max min [WITHSCORES] [LIMIT offset count]
    /// </summary>
    /// <returns></returns>
    Task<Dictionary<string, double>> SortedSetRangeByScoreWithScores(string key, 
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true);

    /// <inheritdoc cref="SortedSetRangeByScoreWithScores"/>
    Task<Dictionary<T, double>> SortedSetRangeByScoreWithScores<T>(string key,
        double min = double.NegativeInfinity, double max = double.PositiveInfinity,
        long offset = 0L, long? count = null, bool orderByAsc = true);

    /// <summary>
    /// 返回有序集中指定（排名）区间内的成员
    /// <code> 
    /// 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
    /// 也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 
    /// </code>
    /// <br/>zrange key start stop [WITHSCORES]
    /// <br/>zrevrange key start stop [WITHSCORES]
    /// </summary>
    Task<Dictionary<string, double>> SortedSetRangeByRankWithScores(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true);

    /// <inheritdoc cref="SortedSetRangeByRankWithScores"/>
    Task<Dictionary<T, double>> SortedSetRangeByRankWithScores<T>(string key,
        long start = 0L, long stop = -1L, bool orderByAsc = true);


    /// <summary>
    /// zrem key member [member ...]
    /// </summary>
    Task<long> SortedSetRemoveAsync<T>(string key, params T[] members);
    /// <inheritdoc cref="SortedSetRemoveAsync"/>
    Task<long> SortedSetRemoveAsync(string key, params string[] members);

    /// <summary>
    /// 用于移除有序集中指定分数（score）区间内的所有成员
    /// <br/>zremrangebyscore key min max
    /// </summary>
    Task<long> SortedSetRemoveByScoreAsync(string key, double min, double max);
    /// <summary>
    /// 用于移除有序集中指定（排名）区间内的所有成员
    /// <br/>zremrangebyrank key start stop
    /// </summary>
    Task<long> SortedSetRemoveByRankAsync(string key, long start, long stop);


    /// <summary>
    /// 有序集合中对指定成员的分数加上增量
    /// <br/>zincrby key increment member
    /// </summary>
    Task<double> SortedSetIncrByAsync<T>(string key, T member, double value = 1);
    /// <inheritdoc cref="SortedSetIncrByAsync"/>
    Task<double> SortedSetIncrByAsync(string key, string member, double value = 1);
}

public partial interface IRedisClient
{
    /// <summary>直接执行lua-script</summary>
    Task<object> EvalLuaAsync(string script, string key, params object[] args);

    Task<object> EvalLuaAsync(string script, string[] keys, object[] args);
}
