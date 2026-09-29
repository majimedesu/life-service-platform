# 高并发生活服务平台（life-service-platform）

基于 Spring Boot + Redis 的生活服务类平台后端，围绕 **店铺浏览 → 探店笔记 → 关注互动 → 优惠券秒杀** 的消费闭环，
重点解决高并发场景下的 **登录态共享、缓存一致性、缓存穿透与击穿、秒杀超卖与一人一单、下单异步化** 等问题。

## 技术栈

| 分类 | 选型 |
|---|---|
| 基础框架 | Spring Boot 2.3.12、Java 8 |
| 持久层 | MyBatis-Plus、MySQL |
| 缓存 | Redis（String / Hash / Set / ZSet / GEO / Bitmap）、Lettuce 连接池 |
| 分布式锁 | 基于 Lua 的自实现 `SimpleRedisLock`；Redisson 配置亦已就绪 |
| 消息队列 | RabbitMQ（直接交换机 + 消息 TTL + 死信队列 + 手动 ACK） |
| 其他 | Hutool、Lombok、Spring AOP |

## 功能模块

| 模块 | 主要类 | 说明 |
|---|---|---|
| 登录鉴权 | `UserController`、`RefreshTokenInterceptor`、`LoginInterceptor` | 短信验证码登录；登录态存 Redis 实现集群 Session 共享；两层拦截器分别负责刷新有效期与登录校验 |
| 店铺查询与缓存 | `ShopController`、`ShopTypeController`、`CacheClient` | Cache Aside 读写策略；空值缓存防穿透；逻辑过期 + 互斥锁 + 线程池异步重建防击穿 |
| 优惠券秒杀 | `VoucherController`、`VoucherOrderController`、`seckill.lua`、`SeckillVoucherListener` | Lua 原子校验库存与一人一单；全局唯一订单号；RabbitMQ 异步落库 |
| 探店笔记与点赞 | `BlogController`、`BlogCommentsController`、`BlogServiceImpl` | 点赞用 ZSet 存储（时间戳为 score），可按点赞时间排序形成排行榜 |
| 好友关注 | `FollowController`、`FollowServiceImpl` | Set 存储关注关系，集合交集求共同关注 |
| 附近店铺 | `ShopServiceImpl` | GEO 存储店铺坐标，按距离排序分页查询 |
| 用户签到 | `UserController`、`UserServiceImpl` | Bitmap 按月记录签到，`BITFIELD` 取本月位图统计连续签到天数 |
| 图片上传 | `UploadController` | 笔记图片上传与删除 |

## 关键实现

### 1. 登录态与集群 Session 共享

登录成功后生成随机 token，以 `login:token:{token}` 为 key 用 Hash 结构存用户信息到 Redis，并设置有效期。
请求进入时由两层拦截器处理：

- `RefreshTokenInterceptor`：拦截所有路径，取出 token 对应的用户放入 `UserHolder`（ThreadLocal），并刷新 token 有效期；
- `LoginInterceptor`：只拦截需要登录的路径，未登录直接返回 401。

这样既避免了多台 Tomcat 之间 Session 不共享的问题，也避免了每个请求都做多余的登录校验。

### 2. 缓存一致性

`CacheClient` 封装了统一的缓存读写：

- **写策略**：Cache Aside —— 先更新数据库，再删除缓存；
- **防穿透**（`queryWithPassThrough`）：数据库查询为空时缓存空字符串并设置较短 TTL（`CACHE_NULL_TTL`），避免同一个不存在的 id 反复打到数据库；
- **防击穿**（`queryWithLogicalExpire`）：热点数据不设物理过期时间，而是在 value 中包裹逻辑过期时间（`RedisData`）。
  命中后若发现逻辑过期，先用互斥锁保证只有一个线程去重建，重建任务提交给独立的线程池（`CACHE_REBUILD_EXECUTOR`）执行，请求线程直接返回旧数据，不阻塞。

### 3. 秒杀：Lua 原子校验 + 异步下单

**第一步：Redis 中的资格校验（`seckill.lua`）**，一次脚本执行内完成，避免多命令之间的竞态：

```lua
-- 库存判断 → 一人一单判断（SISMEMBER）→ 扣减库存（INCRBY）→ 记录下单用户（SADD）
-- 返回 0：校验通过；1：库存不足；2：重复下单
```

调用方 `VoucherOrderServiceImpl#seckillVoucher` 依据返回值区分失败原因，并把「库存不足」「不能重复下单」直接返回给前端。

**第二步：异步落库。** 校验通过后订单消息发往 RabbitMQ 的普通队列，由 `SeckillVoucherListener` 消费：

- 队列侧配置：普通交换机 `X` → 队列 `QA`（消息 TTL 10s，绑定死信交换机 `Y` / 路由键 `YD`）→ 死信队列 `QD`；
- 消费者手动确认：处理成功 `basicAck`，异常 `basicNack(requeue=false)` 丢弃，避免毒消息无限重投；死信队列同样有消费者兜底；
- 落库侧用 `TransactionTemplate` 把「订单落库 + 扣减库存」放在同一事务中，扣减使用
  `stock = stock - 1 where voucher_id = ? and stock > 0` 做乐观锁，避免超卖；
- 消费前先按订单 id 判重（`getById`）保证幂等，重复消费不会产生重复订单。

**其他细节**

- 订单号由 `RedisIdWorker` 生成：`(当前秒数 - 起始时间戳) << 32 | 序列号`，全局唯一且趋势递增；
- 秒杀券库存预热：应用启动时 `@PostConstruct` 把数据库中的秒杀券库存写入 Redis，使用 `setIfAbsent` 以免覆盖运行中已被扣减的实时库存；
- 秒杀开始/结束时间在入参校验阶段拦截（`checkTimeWindow`）。

## 目录结构

```
src/main/java/com/hmdp
├── config        # MvcConfig / MybatisConfig / QueueConfig（RabbitMQ）/ RedissonConfig / 全局异常处理
├── controller    # 接口层：用户、店铺、优惠券、订单、笔记、关注、上传
├── interceptor   # 两层登录拦截器
├── listener      # RabbitMQ 订单消费者
├── service       # 业务接口与实现（impl）
├── mapper        # MyBatis-Plus Mapper
├── entity / dto  # 实体类与传输对象
└── utils         # CacheClient / RedisIdWorker / RedisConstants / 分布式锁 / ThreadLocal 上下文
src/main/resources
├── application.yaml
├── seckill.lua / unLock.lua
├── db/hmdp.sql   # 建库建表与基础数据
└── mapper/       # 自定义 SQL
```

## 本地运行

1. 环境：JDK 8、Maven、MySQL、Redis、RabbitMQ；
2. 导入数据：`mysql -uroot -p < src/main/resources/db/hmdp.sql`；
3. 配置：MySQL 密码通过环境变量 `MYSQL_PASSWORD` 注入（未设置时默认 `root`），Redis、RabbitMQ 默认连接本机；
4. 启动 `HmDianPingApplication`，服务端口 `8081`。

## 数据表

`tb_user`、`tb_user_info`、`tb_shop`、`tb_shop_type`、`tb_voucher`、`tb_seckill_voucher`、`tb_voucher_order`、
`tb_blog`、`tb_blog_comments`、`tb_follow`、`tb_sign`

## 测试

| 测试类 | 用途 |
|---|---|
| `HmDianPingApplicationTests` | 店铺数据写入 Redis GEO、全局唯一 ID 生成 |
| `ShopCacheTest` | 把所有店铺按「逻辑过期」方式预热进 Redis |
| `VoucherOrderControllerTest` | 并发登录 1000 个用户并输出 token 到文件，供 JMeter 对秒杀接口做压测 |

## 后续可优化点

- `seckill.lua` 末尾保留了一处写 `stream.orders` 的 `XADD`，当前异步链路由 RabbitMQ 承担、没有对应的 Stream 消费者，可以清理掉；
- `SimpleRedisLock` 引用的脚本名为 `unlock.lua`，仓库内实际文件名是 `unLock.lua`，在区分大小写的 Linux 环境下会加载失败；
- 缓存 TTL 目前使用固定常量（`CACHE_SHOP_TTL`、`CACHE_NULL_TTL`），可叠加随机量以避免同一时刻集中失效。
