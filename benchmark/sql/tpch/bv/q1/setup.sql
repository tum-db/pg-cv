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

create table if not exists bv.q1_result (
    l_returnflag   char(1) not null,
    l_linestatus   char(1) not null,
    sum_qty        numeric,
    sum_base_price numeric,
    sum_disc_price numeric,
    sum_charge     numeric,
    sum_disc       numeric,
    count_order    bigint
);
create index on bv.q1_result (l_returnflag, l_linestatus);

create or replace function bv.lineitem_insert() returns void
    language plpgsql
as
$$
declare
    tuple       bv.q1_result;
    found_tuple bv.q1_result;
begin
    for tuple in select l_returnflag,
                        l_linestatus,
                        sum(l_quantity)                                       as sum_qty,
                        sum(l_extendedprice)                                  as sum_base_price,
                        sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
                        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                        sum(l_discount)                                       as sum_disc,
                        count(*)                                              as count_order
                 from bv.lineitem
                 where l_shipdate <= date '1998-12-01' - interval '90' day
                 group by l_returnflag, l_linestatus
    loop
        select *
        into found_tuple
        from bv.q1_result
        where bv.q1_result.l_returnflag = tuple.l_returnflag
          and bv.q1_result.l_linestatus = tuple.l_linestatus;

        if not found then
            insert into bv.q1_result
            select tuple.*;
        else
            update bv.q1_result
            set sum_qty        = tuple.sum_qty + found_tuple.sum_qty,
                sum_base_price = tuple.sum_base_price + found_tuple.sum_base_price,
                sum_disc_price = tuple.sum_disc_price + found_tuple.sum_disc_price,
                sum_charge     = tuple.sum_charge + found_tuple.sum_charge,
                sum_disc       = tuple.sum_disc + found_tuple.sum_disc,
                count_order    = tuple.count_order + found_tuple.count_order
            where bv.q1_result.l_returnflag = tuple.l_returnflag
              and bv.q1_result.l_linestatus = tuple.l_linestatus;
        end if;
    end loop;

    truncate bv.lineitem;
end
$$;
