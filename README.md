# graph

个人使用rust实现的图数据库,以学习为主要目的。目前还处在相对原始的beta状态,尚有很多未完成的功能点

## 项目动机

日常中使用的传统关系数据库在面对对象之间的关联关系的时候显得相当的力不从心,需要通过冗余的表关联join来达到效果,显然使用图数据是不错的解决方式<br>
市面上相应的产品也有不少,例如neo4j、nebula等,不过还是想亲自动手实现

## 交互方式

使用websocket,可以同时支持传统后台应用和web前端直连

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

## 创建普通表 create table

```sql
create table user if not exist (id integer,name string null);
create table car if not exist (id integer,color string);
create table tyre if not exist (id integer, name string);
```

## 创建关系 create relation

```sql
create relation usage (number integer);
create relation own (number integer);
```

## 向普通表添加数据

```sql
insert into user values (1,'tom');

insert into car values (34,'red');
insert into car values (43,'red');

insert into tyre values(7,'stone');
```

## 使用关系(relation)连接普通表上的数据

```sql
link user(id =1) to car(color='red') by usage(number = 12);
link car (id =34) to tyre(name ='stone') by own(number=1);
link user(id =1) to car(id =43) by usage(number = 17);
```

## 撤销关系(developing)

```sql
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

```text
select user(id=1 and 0=0) -usage(number > 0) as usage0-> car
```

```json
[
  {
    "car": [
      {
        "color": "red",
        "id": 34
      },
      {
        "color": "red",
        "id": 43
      }
    ],
    "usage0": {
      "number": 12
    },
    "user": [
      {
        "id": 1,
        "name": "tom"
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

```text
select user[id, name](id=1 and 0=0) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre
```

```json
[
  {
    "car": [
      {
        "color": "red",
        "id": 34
      }
    ],
    "own": {
      "dest": {
        "PointDesc": {
          "positions": [
            0
          ],
          "tableName": "tyre"
        }
      },
      "number": {
        "Integer": 1
      },
      "src": {
        "PointDesc": {
          "positions": [
            0
          ],
          "tableName": "car"
        }
      }
    },
    "tyre": [
      [
        0,
        {
          "id": {
            "Integer": 7
          },
          "name": {
            "String": "stone"
          }
        }
      ]
    ],
    "usage0": {
      "number": 12
    },
    "user0": [
      {
        "id": 1,
        "name": "tom"
      }
    ]
  }
]
```

## 删除普通表的数据 delete

```text
delete from user(id=1)
```

## 更新普通表的数据 update

```text
update user[name ='tom0'](id=1)
```