create database demodata;
GO
use demodata;
GO
create table fake_data (
    healthcare nvarchar(max),
    email nvarchar(max),
    btc nvarchar(max),
    mandarin nvarchar(max),
    uri nvarchar(max),
    truthiness nvarchar(max)
);
GO
create table bulk_data (
    id int identity primary key,
    healthcare nvarchar(max),
    email nvarchar(max),
    btc nvarchar(max),
    mandarin nvarchar(max),
    uri nvarchar(max),
    truthiness nvarchar(max),
    quantity int,
    mass float,
    faked_at datetime
);
GO
create database alternate;
GO
use alternate;
GO
create table hey (
    col1 int,
    col2 int,
    col3 int,
    constraint pk_yo primary key ( col1, col2, col3)
);
GO
insert into hey ( col1, col2, col3 )
values
    (1,1,1),
    (1,1,2),
    (1,1,3),
    (2,1,1),
    (2,1,2),
    (2,1,3),
    (2,2,1),
    (2,2,2),
    (2,2,3),
    (3,1,1),
    (3,1,2),
    (3,1,3),
    (3,2,1),
    (3,2,2),
    (3,2,3),
    (3,3,1),
    (3,3,2),
    (3,3,3)
GO