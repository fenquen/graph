# graph

## create table

```sql
create table user (id integer,name string);
create table car (id integer,color string);
```

## create relation

```sql
create relation usage (number integer);
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
```

## query

```sql
select user[id,name](id=1 and 1=1) -usage(number > 9) as usage0-> car
```

```json
{
    "user": [
        {
            "id": 1,
            "name": "tom",
            "usage0": {
                "number": 12,
                "car": [
                    {
                        "id": 34,
                        "color": "red"
                    }
                ]
            }
        },
        {
            "id": 34,
            "name": "tom",
            "usage0": {
                "number": 17,
                "car": [
                    {
                        "id": 43,
                        "color": "red"
                    }
                ]
            }
        }
    ]
}
```

## delete

## update