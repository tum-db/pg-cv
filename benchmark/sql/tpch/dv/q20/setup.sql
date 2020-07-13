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

create index on dv.lineitem (l_partkey, l_suppkey);

create materialized view dv.q20 as
    select s_name, s_address
    from supplier, nation
    where s_suppkey in (select ps_suppkey
                        from partsupp
                        where ps_partkey in (select p_partkey
                                             from part
                                             where p_name like 'forest%')
                          and ps_availqty > (select 0.5 * sum(l_quantity)
                                             from dv.lineitem
                                             where l_partkey = ps_partkey
                                               and l_suppkey = ps_suppkey
                                               and l_shipdate >= date '1994-01-01'
                                               and l_shipdate < date '1995-01-01'))
      and s_nationkey = n_nationkey
      and n_name = 'CANADA';
