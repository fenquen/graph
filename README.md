# graph

## create table

```sql
create table user (id integer,name string);insert into user values (1,'tom');
create table car (id integer,color string);insert into car values (34,'red');insert into car values (43,'red');
create table tyre (id integer, name string);insert into tyre values(7,'stone');
```

## create relation

```sql
create relation usage (number integer);
create relation own (number integer);
```

## insert

```sql
insert into user values (1,'tom');

insert into car values (34,'red');
insert into car values (43,'red');
```

## link with relation

```sql
link user(id =1) to car(color='red') by usage(number = 12);
link user(id =1) to car(id =43) by usage(number = 17);
link car (id =34) to tyre(name ='stone') by own(number=1);
```

## query

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

developing

## update

developing