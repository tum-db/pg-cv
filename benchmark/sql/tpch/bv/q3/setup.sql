create table bv.lineitem (
    l_orderkey      integer        not null,
    l_partkey       integer        not null,
    l_suppkey       integer        not null,
    l_linenumber    integer        not null,
    l_quantity      decimal(15, 2) not null,
    l_extendedprice decimal(15, 2) not null,
    l_discount      decimal(15, 2) not null,
    l_tax           decimal(15, 2) not null,
    l_returnflag    char(1)        not null,
    l_linestatus    char(1)        not null,
    l_shipdate      date           not null,
    l_commitdate    date           not null,
    l_receiptdate   date           not null,
    l_shipinstruct  char(25)       not null,
    l_shipmode      char(10)       not null,
    l_comment       varchar(44)    not null
);

create table if not exists bv.q3_result (
    l_orderkey     integer not null,
    revenue        numeric,
    o_orderdate    date    not null,
    o_shippriority integer not null
);
create index on bv.q3_result (l_orderkey);


create or replace function bv.lineitem_insert() returns void
    language plpgsql
as
$$
declare
    tuple       bv.q3_result;
    found_tuple bv.q3_result;
begin
    for tuple in select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
                 from customer, orders, bv.lineitem
                 where c_mktsegment = 'BUILDING'
                   and c_custkey = o_custkey
                   and l_orderkey = o_orderkey
                   and o_orderdate < date '1995-03-15'
                   and l_shipdate > date '1995-03-15'
                 group by l_orderkey, o_orderdate, o_shippriority
    loop
        select *
        into found_tuple
        from bv.q3_result
        where bv.q3_result.l_orderkey = tuple.l_orderkey;

        if not found then
            insert into bv.q3_result
            select tuple.*;
        else
            update bv.q3_result
            set revenue = tuple.revenue + found_tuple.revenue
            where bv.q3_result.l_orderkey = tuple.l_orderkey;
        end if;
    end loop;

    truncate bv.lineitem;
end
$$;
