create table dv.lineitem (
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

create materialized view dv.q1 as
    select l_returnflag,
           l_linestatus,
           sum(l_quantity)                                       as sum_qty,
           sum(l_extendedprice)                                  as sum_base_price,
           sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
           sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
           avg(l_quantity)                                       as avg_qty,
           avg(l_extendedprice)                                  as avg_price,
           avg(l_discount)                                       as avg_disc,
           count(*)                                              as count_order
    from dv.lineitem
    where l_shipdate <= date '1998-12-01' - interval '90' day
    group by l_returnflag, l_linestatus;
