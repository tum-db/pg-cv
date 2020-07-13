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

create table if not exists bv.q15_tmp (
    supplier_no   integer not null,
    total_revenue numeric
);
create index on bv.q15_tmp (supplier_no);

create table if not exists bv.q15_result (
    s_suppkey     integer     not null,
    s_name        char(25)    not null,
    s_address     varchar(40) not null,
    s_phone       char(15)    not null,
    total_revenue numeric
);
create index on bv.q15_result (s_suppkey);

create or replace function bv.lineitem_insert() returns void
    language plpgsql
as
$$
declare
    tuple       bv.q15_tmp;
    found_tuple bv.q15_tmp;
    revenue     numeric;
    max_revenue numeric;
begin
    for tuple in (select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
                  from bv.lineitem
                  where l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-04-01'
                  group by l_suppkey)
    loop
        select *
        into found_tuple
        from bv.q15_tmp
        where bv.q15_tmp.supplier_no = tuple.supplier_no;

        -- insert tuple in tmp relation and keep revenue to determine whether to insert tuple into result
        if not found then
            revenue := tuple.total_revenue;

            insert into bv.q15_tmp
            select tuple.*;
        else
            revenue := tuple.total_revenue + found_tuple.total_revenue;

            update bv.q15_tmp
            set total_revenue = revenue
            where bv.q15_tmp.supplier_no = tuple.supplier_no;
        end if;

        -- get previous max revenue (max revenue in result)
        max_revenue := coalesce((select max(total_revenue)
                                 from bv.q15_result), 0);

        -- truncate result if revenue of new tuple is larger
        if revenue > max_revenue then truncate bv.q15_result; end if;

        -- insert new tuple if revenue is larger or equal
        if revenue >= max_revenue then
            insert into bv.q15_result
            select s_suppkey, s_name, s_address, s_phone, revenue
            from supplier
            where s_suppkey = tuple.supplier_no
              and not exists(select *
                             from bv.q15_result
                             where bv.q15_result.s_suppkey = supplier.s_suppkey);
        end if;
    end loop;

    truncate bv.lineitem;
end
$$;
