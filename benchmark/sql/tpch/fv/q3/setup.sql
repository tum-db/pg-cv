create table if not exists fv.q3_result (
    l_orderkey     integer not null,
    revenue        numeric,
    o_orderdate    date    not null,
    o_shippriority integer not null
);
create index on fv.q3_result (l_orderkey);

create or replace function fv.lineitem_insert(entries stream_lineitem[]) returns void
    language plpgsql
as
$$
declare
    tuple       fv.q3_result;
    found_tuple fv.q3_result;
begin
    for tuple in select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
                 from customer, orders, unnest(entries)
                 where c_mktsegment = 'BUILDING'
                   and c_custkey = o_custkey
                   and l_orderkey = o_orderkey
                   and o_orderdate < date '1995-03-15'
                   and l_shipdate > date '1995-03-15'
                 group by l_orderkey, o_orderdate, o_shippriority
    loop
        select *
        into found_tuple
        from fv.q3_result
        where fv.q3_result.l_orderkey = tuple.l_orderkey;

        if not found then
            insert into fv.q3_result
            select tuple.*;
        else
            update fv.q3_result
            set revenue = tuple.revenue + found_tuple.revenue
            where fv.q3_result.l_orderkey = tuple.l_orderkey;
        end if;
    end loop;
end
$$;
