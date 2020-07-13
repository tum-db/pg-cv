create schema if not exists public;
grant all on schema public to postgres;
grant all on schema public to public;

create table nation (
    n_nationkey integer primary key not null,
    n_name      char(25)            not null,
    n_regionkey integer             not null,
    n_comment   varchar(152)
);
copy nation from '/data/nation.tbl' with (format csv, delimiter '|');

create table region (
    r_regionkey integer primary key not null,
    r_name      char(25)            not null,
    r_comment   varchar(152)
);
copy region from '/data/region.tbl' with (format csv, delimiter '|');

create table part (
    p_partkey     integer primary key not null,
    p_name        varchar(55)         not null,
    p_mfgr        char(25)            not null,
    p_brand       char(10)            not null,
    p_type        varchar(25)         not null,
    p_size        integer             not null,
    p_container   char(10)            not null,
    p_retailprice decimal(15, 2)      not null,
    p_comment     varchar(23)         not null
);
copy part from '/data/part.tbl' with (format csv, delimiter '|');

create table supplier (
    s_suppkey   integer primary key not null,
    s_name      char(25)            not null,
    s_address   varchar(40)         not null,
    s_nationkey integer             not null,
    s_phone     char(15)            not null,
    s_acctbal   decimal(15, 2)      not null,
    s_comment   varchar(101)        not null
);
copy supplier from '/data/supplier.tbl' with (format csv, delimiter '|');

create table partsupp (
    ps_partkey    integer        not null,
    ps_suppkey    integer        not null,
    ps_availqty   integer        not null,
    ps_supplycost decimal(15, 2) not null,
    ps_comment    varchar(199)   not null,
    primary key (ps_partkey, ps_suppkey)
);
copy partsupp from '/data/partsupp.tbl' with (format csv, delimiter '|');

create table customer (
    c_custkey    integer primary key not null,
    c_name       varchar(25)         not null,
    c_address    varchar(40)         not null,
    c_nationkey  integer             not null,
    c_phone      char(15)            not null,
    c_acctbal    decimal(15, 2)      not null,
    c_mktsegment char(10)            not null,
    c_comment    varchar(117)        not null
);
copy customer from '/data/customer.tbl' with (format csv, delimiter '|');

create table orders (
    o_orderkey      integer primary key not null,
    o_custkey       integer             not null,
    o_orderstatus   char(1)             not null,
    o_totalprice    decimal(15, 2)      not null,
    o_orderdate     date                not null,
    o_orderpriority char(15)            not null,
    o_clerk         char(15)            not null,
    o_shippriority  integer             not null,
    o_comment       varchar(79)         not null
);
copy orders from '/data/orders.tbl' with (format csv, delimiter '|');

create table base_lineitem (
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
    l_comment       varchar(44)    not null,
    primary key (l_orderkey, l_linenumber)
);
copy base_lineitem from '/data/lineitem.tbl' with (format csv, delimiter '|');
alter table base_lineitem
    add counter serial;
create index on base_lineitem (counter);

alter table supplier
    add foreign key (s_nationkey) references nation (n_nationkey);

alter table partsupp
    add foreign key (ps_partkey) references part (p_partkey);
alter table partsupp
    add foreign key (ps_suppkey) references supplier (s_suppkey);

alter table customer
    add foreign key (c_nationkey) references nation (n_nationkey);

alter table orders
    add foreign key (o_custkey) references customer (c_custkey);

alter table nation
    add foreign key (n_regionkey) references region (r_regionkey);

alter table base_lineitem
    add foreign key (l_orderkey) references orders (o_orderkey);
alter table base_lineitem
    add foreign key (l_partkey, l_suppkey) references partsupp (ps_partkey, ps_suppkey);

create table stream_lineitem (
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
