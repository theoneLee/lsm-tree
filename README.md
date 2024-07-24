### 简介
这个是一个练手项目，使用lsm tree实现一个kv的数据库。

### 流程图
![图片](lsmtree.png)


### quick start
`make run`

### TODO
1. 使用read through 的方式进行sstable的cache
2. memtable新增跳表实现
3. sstable 查询增加布隆过滤器
4. 优化sstable合并策略
