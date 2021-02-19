/*
TODO: this is the basis for checking how big a row is on a SQL table,
and using that size to determine the best batch size for extracting that table in chunks
*/

declare @table nvarchar(128)
declare @idcol nvarchar(128)
declare @sql nvarchar(max)


set @table = 'bulk_data'
set @idcol = 'id'

set @sql = 'select ' + char(39) + @table + char(39) +' as table_name, avg(0'

select @sql = @sql + ' + isnull(datalength(' + name + '), 1)' 
        from  sys.columns 
        where object_id = object_id(@table)
        and   is_computed = 0
set @sql = @sql + ') as rowsize from (select top 1000 * from ' + @table + ' ) as table_sample'

PRINT @sql

exec (@sql)
