# graph

个人使用rust实现的rdbms风格的图数据库 <br>
<a href="https://github.com/fenquen/graph">项目地址</a> <a href="https://github.com/fenquen">个人的github主页</a> <br>

## 项目动机

日常使用的传统关系数据库在面对对象之间的关联关系的时候显得相当的力不从心，通常需要通过大量冗余的表关联join来达到效果<br>
显然使用图数据是不错的解决方式，市面上相应的产品也有不少，例如neo4j、nebula等<br>
<br>
rust是门相当特别的编程语言，使用独特的内存体系实现了不需程序员手动管理内存的无gc，大大降低了像c/c++的内存泄漏的风险<br>
使用rust编写1个自己的图数据库是个相当有趣和有挑战的项目<br>
<br>
如果您感觉还可以对你有些帮助还望不吝惜你的star，你们的肯定是最大的支持和动力

## 开发环境

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

使用websocket，默认端口9673，可以支持传统后台应用和web前端直连，后续有时间的话会编写java、rust、go客户端

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

## 使用说明

### 创建表

```sql
create table user if not exist (id integer,name string null);
create table car if not exist (id integer,color string);
create table tyre if not exist (id integer, name string);
```

### 创建关系

```sql
--对象之间的使用关系
create relation usage (number integer);

--对象之间的拥有关系
create relation own (number integer);
```

### 向表添加数据

```sql
insert into user values (1,'tom');

insert into car values (34,'red');
insert into car values (43,'red');

insert into tyre values(7,'stone');
```

### 使用关系连接表上的数据

```sql
--id是1的user 使用(usage)12辆 color是'red的car
link user(id =1) to car(color='red') by usage(number = 12);

--id是34的car 拥有(own)1个 name是'stone'的tyre 
link car (id =34) to tyre(name ='stone') by own(number=1);

--id是1的user 使用(usage)17辆 id是17的car
link user(id =1) to car(id =43) by usage(number = 17);
```

### 撤销关系

撤销 id是34的car 拥有(own)1个 name是'stone'的tyre

```sql
unlink car (id =34) to tyre(name ='stone') by own(number=1);
```

### 查询

#### 普通查询

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

#### 关系查询

##### 单关系查询

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

##### 多关系查询

搜索满足如下的脉络联系 <br>
id是1的user && 对car的使用(usage)数量(number) >9 && 相应的car对tyre的拥有(own)数量(number)是1,user只显示id和name

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

##### 通过关系筛选数据

筛选满足如下条件的user <br>
id是1 <br>
拥有属性number大于7的usage关系，是start和end都可以 <br>
拥有属性number=7的own关系，是end端

```text
select user(id = 1 ) as user0 ,in usage (number > 7) ,as end in own(number =7)
```

```json
[
  {
    "name": "tom",
    "id": 1
  }
]
```

##### 关系的深度查询

例如user之间存在likes关系如下

```text
create table if not exist user (id integer,name string)

insert into user values (1,'tom0')
insert into user values (2,'tom2')
insert into user values (3,'tom3')
insert into user values (4,'tom4')
insert into user values (5,'tom5')
insert into user values (6,'tom6')
insert into user values (7,'tom7')

create relation likes(level integer)

link user(id =1) to user(id=2) by likes(level =0)
link user(id =2) to user(id=3) by likes(level =0)
link user(id =3) to user(id=4) by likes(level =0)
link user(id =4) to user(id=5) by likes(level =0)
link user(id =5) to user(id=6) by likes(level =0)
link user(id= 6) to user(id=7) by likes(level =0)
```

现要查询likes深度是6层的user

```text
select user as user0 -likes recursive[6..6]-> user as user1
```

```json
[
  {
    "user0": [
      {
        "id": 1,
        "name": "tom0"
      }
    ],
    "user1": [
      {
        "id": 7,
        "name": "tom7"
      }
    ]
  }
]
```

### 删除普通表的数据

删掉id是1的user

```text
delete from user(id=1)
```

### 更新普通表的数据

update id是1的user，将name设为'tom0'

```text
update user[name ='tom0'](id=1)
```

如满足条件的user已经被关联到了某个关系上(调用上述 link user(id =1) to car(color='red') by usage( number = 12)) <br>
那么update是不被允许的，会产生如下的错误，因为用户对数据的更改会违背关联关系设立的意义

```json
{
  "success": false,
  "errorMsg": "graph error message:update can not execute, because data has been linked",
  "data": null
}
```