using Common;
using Common.JsonNet.v2;
using CSRedis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Program_ext;

partial class Program_tx_redis_client
{
    /**
     * for 
    
     *      lua-script在服务器上的sha1其实就是该脚本字符串的sha1

     */

    public static void ConfigureServices(IServiceCollection services, IConfiguration config)
    {
        
    }

    [rg.DInject] IServiceProvider services;
    [rg.DInject] ILogger log;
    [rg.DInject] IHostApplicationLifetime lifetime;

    public async Task OnRunAsync()
    {
        ThreadPool.SetMinThreads(200, 200);

        #region CSRedisCore
        /*
        var rredis = new Common.Redis.CSRedisCore.MyRedisClient(
            new CSRedisClient(
                //"10.1.0.7:6379,password=SxbLucas$0769,defaultDatabase=15,poolsize=50,preheat=10,connectTimeout=10000,syncTimeout=10000,asyncTimeout=10000," + "asyncPipeline=false,testcluster=false,"
                "127.0.0.1:6379,password=,defaultDatabase=0,poolsize=50,preheat=10,connectTimeout=10000,syncTimeout=10000,asyncTimeout=10000," + "asyncPipeline=false,testcluster=false,"
            ),
            (obj) => obj.ToJsonStr(true),
            (str, type) => str.JsonStrTo(type)
        );
        //*/
        #endregion

        #region StackExchange.Redis
        //*
        var rredis = new Common.Redis.StackExchangeRedis.MyRedisClient(
            //ConfigurationOptions.Parse(@"10.1.0.7:6379,DefaultDatabase=12,password=SxbLucas$0769,syncTimeout=10000,asyncTimeout=10000,connectTimeout=10000,responseTimeout=10000,connectRetry=3,"),
            ConfigurationOptions.Parse(@"127.0.0.1:6379,DefaultDatabase=0,password=,syncTimeout=10000,asyncTimeout=10000,connectTimeout=10000,responseTimeout=10000,connectRetry=3,"),
            funcSerialize: (obj) => obj.ToJsonStr(true),
            funcDeserialize: (str, type) => str.JsonStrTo(type)
        );
        //*/
        #endregion

        // test
        {
            var k_ttl = "sdadad";
            var ttl = await rredis.TtlAsync(k_ttl);

            var tsobj = await rredis.GetAsync<TsObj>("a:k1");

            var tsobj_json = await rredis.GetAsync("a:k1");

            await rredis.SetAsync("a:k1", new TsObj { Bb = true, Id = "adasdsadsad" }, 60 * 30);
            await rredis.SetAsync("a:k1", "123", 60 * 30, nx: true);
            await rredis.SetAsync("a:k1", null, 60 * 30, nx: false);

            await rredis.IncrByAsync("a:k2", -1);

            await rredis.ExpireAsync("a:k2", TimeSpan.FromMilliseconds(60099.54));

            await rredis.DeleteAsync("a:k1", "a:k2");

            log.LogInformation("hash");
            Debugger.Break();

            await rredis.HashSetAsync("a:k3", new Dictionary<string, string> 
            {
                ["1"] = "111",
                ["22"] = "b22"
            }, TimeSpan.FromSeconds(60));

            log.LogInformation("sortset Zxxx");
            Debugger.Break();

            var i_z2 = await rredis.SortedSetAddAsync("z2", ("a|b|c", 1), ("2|2|22", 2));
            i_z2 = await rredis.SortedSetAddAsync<string>("z2", ("a|b|c", 3), ("2|2|22", 4));
            var len_sortset = await rredis.SortedSetCountByScoreAsync("z1", -1, 3.2);
            var i_zrem = await rredis.SortedSetRemoveByScoreAsync("z1", 4, 100);
            var z1 = await rredis.SortedSetRangeByRankWithScores("z1");

            log.LogInformation("eval lua");
            Debugger.Break();

            var lua1 = """
                local ttl = redis.call('ttl', KEYS[1])
                if (ttl > -2) then
                    local stock = tonumber(redis.call('hget', KEYS[1], 'stock1'))
                    if (stock == -1) then
                        return -1
                    end
                    local num = tonumber(ARGV[1])
                    if (ttl > -1) and (ttl < 30) then
                        redis.call('expire', KEYS[1], ARGV[2])
                    end
                    if (num == 0) then
                        return stock
                    elseif (num > 0) then
                	    if (stock >= num) then
                            return redis.call('hincrby', KEYS[1], 'stock1', 0 - num)
                        end
                        return -2
                    else
                        local stock1 = stock - num
                	    stock = tonumber(redis.call('hget', KEYS[1], 'stock0'))
                	    if (stock >= stock1) then
                		    redis.call('hset', KEYS[1], 'stock1', stock1)
                		    return stock1
                	    end
                        redis.call('hset', KEYS[1], 'stock1', stock)
                	    return stock
                    end
                end
                return -3
                """;
            var res_lua1 = await rredis.EvalLuaAsync(lua1, "lua1", Pargs0(-1000, 600));
            var res_lua2 = await rredis.EvalLuaAsync(lua1, new[] { "lua1", "nil" }, Pargs0(-1000, 600));
            var c_lua1 = Convert.ToInt32(res_lua1);
            var c_lua1_2 = Convert.ToInt32(res_lua2);

            if (rredis is Common.Redis.StackExchangeRedis.MyRedisClient)
            {
                Debugger.Break();
                var db = rredis.GetRaw<IDatabase>();
                var sha1Lua1 = HashAlgmUtil.Encrypt(lua1, HashAlgmUtil.Sha1);
                var r = await db.ScriptEvaluateAsync(sha1Lua1, new RedisKey[] { "lua1" }, new RedisValue[] { -1000, 600 });
                var c_lua2 = Convert.ToInt32(r);
                Debug.Assert(c_lua1 == c_lua2);
            }
            if (rredis is Common.Redis.CSRedisCore.MyRedisClient)
            {
                Debugger.Break();
                var db = rredis.GetRaw<CSRedisClient>();
                var sha1Lua1 = HashAlgmUtil.Encrypt(lua1, HashAlgmUtil.Sha1);
                //var sh2 = await db.ScriptLoadAsync(lua1);
                var sh2 = db.ScriptLoad(lua1); await Task.Delay(1000);
                Debug.Assert(sha1Lua1 == sh2);
                var b = await db.ScriptExistsAsync(lua1);
                Debug.Assert(b.All(_ => _ == true)); //?? false
                var r = await db.EvalSHAAsync(sha1Lua1, "lua1", new[] { -1000, 600 }); //?? error
                var c_lua2 = Convert.ToInt32(r);
                Debug.Assert(c_lua1 == c_lua2);
            }

            log.LogInformation("exec pipeline");
            Debugger.Break();
            {
                using var pipe = rredis.NewPipeline();
                pipe.AddRaw<IBatch, string>(out var t1, batch => batch.StringGetAsync("a:k1").ContinueWith(t => (string)t.Result));
                pipe.AddRaw((IBatch batch) => batch.ExecuteAsync("TTL", "a:k1").Tto(v => (long)v));
                var res = await pipe.ExecuteAsync();
                await t1;
                Debug.Assert(Equals(res[0], t1.Result));
                Debugger.Break();
                pipe.ExpireAsync("a:k3", 60)
                    .TtlAsync("a:k3")
                    .HashGetAllAsync<string>("a:k3")
                    .SortedSetRangeByScoreWithScores("z1", 1, 3)
                    .Self(out _);
                res = await pipe.ExecuteAsync();
                Debugger.Break();
            }

            // end
            Debugger.Break();
        }        
    }

    class TsObj
    {
        public string Id { get; set; }
        public int I { get; set; }
        public bool Bb { get; set; }
    }
}
