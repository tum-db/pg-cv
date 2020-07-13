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

create table if not exists ev.q15_tmp (
    supplier_no   integer not null,
    total_revenue numeric
);
create index on ev.q15_tmp (supplier_no);

create table if not exists ev.q15_result (
    s_suppkey     integer     not null,
    s_name        char(25)    not null,
    s_address     varchar(40) not null,
    s_phone       char(15)    not null,
    total_revenue numeric
);
create index on ev.q15_result (s_suppkey);

create or replace function ev.lineitem_insert() returns trigger
    language plpgsql
as
$$
declare
    tuple       ev.q15_tmp;
    found_tuple ev.q15_tmp;
    revenue     numeric;
    max_revenue numeric;
begin
    for tuple in (select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
                  from lineitem_transition
                  where l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-04-01'
                  group by l_suppkey)
    loop
        select *
        into found_tuple
        from ev.q15_tmp
        where ev.q15_tmp.supplier_no = tuple.supplier_no;

        -- insert tuple in tmp relation and keep revenue to determine whether to insert tuple into result
        if not found then
            revenue := tuple.total_revenue;

            insert into ev.q15_tmp
            select tuple.*;
        else
            revenue := tuple.total_revenue + found_tuple.total_revenue;

            update ev.q15_tmp
            set total_revenue = revenue
            where ev.q15_tmp.supplier_no = tuple.supplier_no;
        end if;

        -- get previous max revenue (max revenue in result)
        max_revenue := coalesce((select max(total_revenue)
                                 from ev.q15_result), 0);

        -- truncate result if revenue of new tuple is larger
        if revenue > max_revenue then truncate ev.q15_result; end if;

        -- insert new tuple if revenue is larger or equal
        if revenue >= max_revenue then
            insert into ev.q15_result
            select s_suppkey, s_name, s_address, s_phone, revenue
            from supplier
            where s_suppkey = tuple.supplier_no
              and not exists(select *
                             from ev.q15_result
                             where ev.q15_result.s_suppkey = supplier.s_suppkey);
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

