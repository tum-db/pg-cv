create table ev.lineitem (
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

create table if not exists ev.q6_result (
    revenue numeric
);

create or replace function ev.lineitem_insert() returns trigger
    language plpgsql
as
$$
declare
    tuple       ev.q6_result;
    found_tuple ev.q6_result;
begin
    for tuple in (select sum(l_extendedprice * l_discount) as revenue
                  from lineitem_transition
                  where l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1995-01-01'
                    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                    and l_quantity < 24)
    loop
        select *
        into found_tuple
        from ev.q6_result;

        if not found then
            insert into ev.q6_result
            select tuple.*;
        else
            update ev.q6_result
            set revenue = coalesce(coalesce(tuple.revenue, 0) + found_tuple.revenue,
                                   tuple.revenue + coalesce(found_tuple.revenue, 0));
        end if;
    end loop;

    return null;
end
$$;

create trigger lineitem_insert_trigger
    after insert
    on ev.lineitem
    referencing new table as lineitem_transition
    for each statement
execute function ev.lineitem_insert();
