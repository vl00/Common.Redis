# Common.Redis

为什么要封装redis客户端？在2019年或者之前，就是刚刚.netcore流行的时候，用StackExchange.Redis会出现严重的超时问题。<br>
此时国内有人推出了CSRedisCore，后来该作者又搞了freeredis。。。而同时StackExchange.Redis超时问题也得到有效改善...<br>
<br>
这个库就是基于这样的原因，封装常用的client api，让项目不用改来改去。。。
