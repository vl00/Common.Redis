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

    /// <summary>��ͨɾ��,��ģ��ɾ��</summary>
    Task<long> DeleteAsync(params string[] keys);

    Task<bool> SetAsync(string key, string value, int expSec = -1, bool nx = false);
    Task<bool> SetAsync<T>(string key, T obj, int expSec = -1, bool nx = false);

    Task<bool> SetNxAsync(string key, string value, int expSec = -1) => SetAsync(key, value, expSec, true);
    Task<bool> SetNxAsync<T>(string key, T obj, int expSec = -1) => SetAsync(key, obj, expSec, true);

    /// <summary>
    /// func(async()=>{...}) �ɷ���Ĭ�϶���ֵ��ֹ����nullʱ��β����ݿ�
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
    /// zadd �����򼯺����һ��������Ա�����߸����Ѵ��ڳ�Ա�ķ���
    /// </summary>
    Task<long> SortedSetAddAsync<T>(string key, params (T, double)[] scoreMembers);
    /// <inheritdoc cref="SortedSetAddAsync"/>
    Task<long> SortedSetAddAsync(string key, params (string, double)[] scoreMembers);

    /// <summary>
    /// zscore ����������,��Ա�ķ���ֵ
    /// </summary>
    Task<double?> SortedSetGetScoreAsync<T>(string key, T member);
    /// <inheritdoc cref="SortedSetGetScoreAsync"/>
    Task<double?> SortedSetGetScoreAsync(string key, string member);

    /// <summary>
    /// zrank ����������ָ����Ա���������������򼯳�Ա������ֵ����(��С����)˳�����С�<br/>
    /// zrevrank ����������ָ����Ա���������������򼯳�Ա������ֵ����(�Ӵ�С)˳�����С�<br/>
    /// </summary>
    Task<long?> SortedSetGetRankAsync<T>(string key, T member, bool orderByAsc = true);
    /// <inheritdoc cref="SortedSetGetRankAsync"/>
    Task<long?> SortedSetGetRankAsync(string key, string member, bool orderByAsc = true);

    /// <summary>
    /// zcard ��ȡ���򼯺ϵĳ�Ա����
    /// </summary>
    Task<long> SortedSetCountAsync(string key);
    /// <summary>
    /// zcount ���ڼ������򼯺���ָ����������ĳ�Ա����
    /// </summary>
    Task<long> SortedSetCountByScoreAsync(string key, double min, double max);


    /// <summary>
    /// ����������ָ��������score�������ڵ����еĳ�Ա
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
    /// ����������ָ���������������ڵĳ�Ա
    /// <code> 
    /// �±���� start �� stop ���� 0 Ϊ�ף�Ҳ����˵���� 0 ��ʾ���򼯵�һ����Ա���� 1 ��ʾ���򼯵ڶ�����Ա���Դ����ơ�
    /// Ҳ����ʹ�ø����±꣬�� -1 ��ʾ���һ����Ա�� -2 ��ʾ�����ڶ�����Ա���Դ����ơ� 
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
    /// �����Ƴ�������ָ��������score�������ڵ����г�Ա
    /// <br/>zremrangebyscore key min max
    /// </summary>
    Task<long> SortedSetRemoveByScoreAsync(string key, double min, double max);
    /// <summary>
    /// �����Ƴ�������ָ���������������ڵ����г�Ա
    /// <br/>zremrangebyrank key start stop
    /// </summary>
    Task<long> SortedSetRemoveByRankAsync(string key, long start, long stop);


    /// <summary>
    /// ���򼯺��ж�ָ����Ա�ķ�����������
    /// <br/>zincrby key increment member
    /// </summary>
    Task<double> SortedSetIncrByAsync<T>(string key, T member, double value = 1);
    /// <inheritdoc cref="SortedSetIncrByAsync"/>
    Task<double> SortedSetIncrByAsync(string key, string member, double value = 1);
}

public partial interface IRedisClient
{
    /// <summary>ֱ��ִ��lua-script</summary>
    Task<object> EvalLuaAsync(string script, string key, params object[] args);

    Task<object> EvalLuaAsync(string script, string[] keys, object[] args);
}
