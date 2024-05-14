# graph

个人使用rust实现的图数据库,以学习为主要目的。目前还处在相对原始的可用状态,尚有很多未完成的功能点

## 项目动机

日常中使用的传统关系数据库在面对对象之间的关联关系的时候显得相当的力不从心,需要通过冗余的表关联join来达到效果,显然使用图数据是不错的解决方式<br>
市面上相应的产品也有不少,例如neo4j、nebula等,不过还是想亲自下场实现

## 交互方式

使用websocket(开发中),这样可以同时支持传统后台应用和web前端

## 创建普通表

```sql
create table user (id integer,name string);insert into user values (1,'tom');
create table car (id integer,color string);insert into car values (34,'red');insert into car values (43,'red');
create table tyre (id integer, name string);insert into tyre values(7,'stone');
```

## 创建关系(relation)

```sql
create relation usage (number integer);
create relation own (number integer);
```

## 向普通表添加数据

```sql
insert into user values (1,'tom');

insert into car values (34,'red');
insert into car values (43,'red');
```

## 使用关系(relation)连接普通表上的数据

```sql
link user(id =1) to car(color='red') by usage(number = 12);
link user(id =1) to car(id =43) by usage(number = 17);
link car (id =34) to tyre(name ='stone') by own(number=1);
```

## 查询

### ordinary qurry

```sql
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

### relation query

```sql
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

```sql
select user[id,name](id=1 and 0=0) as user0 -usage(number > 9) as usage0-> car -own(number=1)-> tyre
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

## delete

```sql
delete from user(id=1)
```

## update

developing