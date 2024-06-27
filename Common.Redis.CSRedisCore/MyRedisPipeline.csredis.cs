using CSRedis;
using CSRedis.Internal.ObjectPool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Common.Redis.CSRedisCore;

public partial class MyRedisClient : IRedisClient
{
    public IRedisPipeline NewPipeline() => throw new NotImplementedException();
}

public partial class MyRedisPipeline //: IRedisPipeline
{ 
}

public partial class MyRedisPipeline
{
}
