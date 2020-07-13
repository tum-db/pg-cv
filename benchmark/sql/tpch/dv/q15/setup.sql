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

create materialized view dv.q15 as
    with revenue as (select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
                     from dv.lineitem
                     where l_shipdate >= date '1996-01-01'
                       and l_shipdate < date '1996-04-01'
                     group by l_suppkey)
    select s_suppkey, s_name, s_address, s_phone, total_revenue
    from supplier, revenue
    where s_suppkey = supplier_no
      and total_revenue = (select max(total_revenue)
                           from revenue);
