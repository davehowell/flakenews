use demodata;
GO
--== range of values for random numerics
declare @startvalue   int,
        @endvalue     int,
        @range        int
;
select  @startvalue   = 1,
        @endvalue     = 5000,
        @range        = @endvalue - @startvalue + 1
;
--== range of values for contiguous time periods
declare @startdate datetime, --inclusive
        @enddate   datetime, --exclusive
        @periods   int
;
select @startdate = '2020-01-01', --inclusive
       @enddate   = '2020-02-11 16:00:00', --exclusive
       @periods   = datediff(hh,@startdate,@enddate)
;
--== range of values for contiguous time periods
with fake_numerics as (
    select top (@periods)
            quantity =  abs(checksum(newid())) % @range + @startvalue,
            mass     = rand(checksum(newid())) * @range + @startvalue,
            faked_at = dateadd(hh,row_number() over (order by (select null))-1,@startdate)
    from sys.all_columns as ac1
    cross join sys.all_columns as ac2
)
insert into bulk_data (
    healthcare,
    email,
    btc,
    mandarin,
    uri,
    truthiness,
    quantity,
    mass,
    faked_at
)
select
    dat.healthcare,
    dat.email,
    dat.btc,
    dat.mandarin,
    dat.uri,
    dat.truthiness,
    num.quantity,
    num.mass,
    num.faked_at
from fake_data as dat
cross join fake_numerics as num
;
