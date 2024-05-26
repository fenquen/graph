# graph

个人使用rust实现的rdbms风格的图数据库，目前还处在相对原始的beta状态

## 项目动机

日常使用的传统关系数据库在面对对象之间的关联关系的时候显得相当的力不从心，通常需要通过大量冗余的表关联join来达到效果<br>
显然使用图数据是不错的解决方式，市面上相应的产品也有不少，例如neo4j、nebula等<br>
rust是门相当特别的编程语言，使用独特的内存体系实现了不需程序员手动管理内存的无gc，大大降低了像c/c++的内存泄漏的风险<br>
使用rust编写1个自己的图数据库是个相当有趣和有挑战的项目

## 开发测试环境

### 硬件

```text
thinkpad x1 nano 2021
    cpu: intel core i5 1130g7
    ram: 16GB
```

### rust版本

```text
2024-04-21后的nightly版本
```

### 操作系统

```text
ubuntu 20.04 with linux kernal 5.15
```

## 参考项目

h2database <br>
<a>https://github.com/h2database/h2database</a><br>

postgresql <br>
<a>https://github.com/postgres/postgres</a><br>

neo4j <br>
<a>https://github.com/neo4j/neo4j</a><br>

tidb <br>
<a>https://github.com/pingcap/tidb</a><br>

tikv <br>
<a>https://github.com/tikv/tikv</a> 


## 交互方式

使用websocket,默认端口9673,可以同时支持传统后台应用和web前端直连,后续有时间的话会编写java、rust、go客户端

### 请求格式

websocket text messge 传递 json 

```json
{
  "requestType": "ExecuteSql",
  "sql": "select user"
}
```

### 返回格式

websocket text message 传递 json

```json
[
  {
    "id": 0,
    "name": "denny"
  }
]
```

## 创建表

```sql
create table user if not exist (id integer,name string null);
create table car if not exist (id integer,color string);
create table tyre if not exist (id integer, name string);
```

## 创建关系

```sql
--对象之间的使用关系
create relation usage (number integer);

--对象之间的拥有关系
create relation own (number integer);
```

## 向表添加数据

```sql
insert into user values (1,'tom');

insert into car values (34,'red');
insert into car values (43,'red');

insert into tyre values(7,'stone');
```

## 使用关系连接表上的数据

```sql
--id是1的user 使用(usage)12辆 color是'red的car
link user(id =1) to car(color='red') by usage(number = 12);

--id是34的car 拥有(own)1个 name是'stone'的tyre 
link car (id =34) to tyre(name ='stone') by own(number=1);

--id是1的user 使用(usage)17辆 id是17的car
link user(id =1) to car(id =43) by usage(number = 17);
```

## 撤销关系

```sql
--撤销 id是34的car 拥有(own)1个 name是'stone'的tyre 
unlink car (id =34) to tyre(name ='stone') by own(number=1);
```

## 查询

### 对普通表的查询 ordinary query

```text
select user(id=1 and 0=0)
```

```json
[
  {
    "name": "tom",
    "id": 1
  }
]
```

### 关系查询 relation query

搜索 id是1的user 对 car的 使用(usage)数量(number)满足>0的情况, 提取它们的全部column

```text
select user(id=1 and 0=0) -usage(number > 0) as usage0-> car
```

返回结果
```json
[
  {
    "user": [
      {
        "id": 1,
        "name": "tom"
      }
    ],
    "usage0": {
      "number": 12
    },
     "car": [
      {
        "color": "red",
        "id": 34
      },
      {
        "color": "red",
        "id": 43
      }
    ]
  },
  {
    "car": [
      {
        "color": "red",
        "id": 43
      }
    ],
    "usage0": {
      "number": 17
    },
    "user": [
      {
        "id": 1,
        "name": "tom"
      }
    ]
  }
]
```

搜索满足如下的脉络联系: id是1的user && 对car的使用(usage)数量(number) >9 && 相应的car对tyre的拥有(own)数量(number)是1, user只显示id和name

```text
select user[id, name](id=1 and 0=0) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre
```

返回结果

```json
[
  {
    "user0": [
      {
        "id": 1,
        "name": "tom"
      }
    ],
    "usage0": {
      "number": 12
    },
    "car": [
      {
        "color": "red",
        "id": 34
      }
    ],
    "own": {
      "number": 1
    },
    "tyre": [
      {
        "id": 7,
        "name": "stone"
      }
    ]
  }
]
```

## 删除普通表的数据 delete

```text
--删掉id是1的user
delete from user(id=1)
```

## 更新普通表的数据 update

```text
--update id是1的user 将name设为'tom0'
update user[name ='tom0'](id=1)
```