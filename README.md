
# Hedis
    实现redis部分协议访问hbase

# 已实现的协议

    kv：
        GET
        SET
        DEL
        EXISTS

    hash:
        HGET
        HSET
        HEXISTS
        HMGET
        HMSET

# 使用方式:

    编译方式:
        cd hedis/
        bash install # 会在当前目录生成bin/hedis-server 二进制文件

    非编译方式:
        hedis/hedis-server --help

    配置:
        cd hedis/conf
        vim hedis.conf
            #注意的配置: (以下的配置是在redis不指定db，hbase的默认配置，同时，default_family是所有新建的hbase表的唯一family字段，使用此服务，新建的表中，只有一个family字段，并且名字为下面配置)
            [hbase_default_table]
            default_namespace           = "redis客户端不指定db(namespace:table)，使用的默认hbase 命名空间，需要手动创建, eg: hedis"
            default_table               = "同default_namespace"
            default_column              = "使用kv相关命令，使用的field名称"
            default_family              = "所有使用该服务的hbase表，都需要建一个与该配置一直的family"

        其他配置看配置文件注释

        配置完成之后，在hbase创建表:

            hbase shell
            hbase(main):004:0* create 'hedis:mtable', 'family'
            0 row(s) in 2.4370 seconds
            => Hbase::Table - hedis:mtable

            hbase(main):005:0> enable 'hedis:mtable'
            0 row(s) in 0.1120 seconds

    启动服务:
        cd hedis/
        bin/hedis-server --conf=conf/hedis.conf

    使用默认的hbase db测试:
       redis-cli -h 127.0.0.1 -p 5555
       127.0.0.1:5555> set key v1
       OK
       127.0.0.1:5555> get key
       "v1"

    通过key指定hbase db测试:
       redis-cli -h 127.0.0.1 -p 5555
       127.0.0.1:5555> set hbase_namespace:hbase_table:hbase_column_family|key value
       OK
       127.0.0.1:5555> get hbase_namespace:hbase_table:hbase_column_family|key
       "value"

       注: 对key的前缀特殊格式(分隔符为半角冒号:):
            指定hbase的[namespace、table、column_family] 
                格式-> namespace:table:family
            指定hbase的[namespace、table]
                格式-> namespace:table
            不支持只指定table&family








