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

create table ev.q20_canadaforestsuppliers as (select s_suppkey, s_name, s_address, ps_partkey, ps_availqty
                                              from nation, supplier, partsupp, part
                                              where n_name = 'CANADA'
                                                and n_nationkey = s_nationkey
                                                and s_suppkey = ps_suppkey
                                                and ps_partkey = p_partkey
                                                and p_name like 'forest%');
create index on ev.q20_canadaforestsuppliers (s_suppkey, ps_partkey);

create table ev.q20_tmp (
    l_partkey    integer not null,
    l_suppkey    integer not null,
    sum_quantity numeric
);
create index on ev.q20_tmp (l_partkey, l_suppkey);

create table ev.q20_result (
    s_suppkey  integer     not null,
    ps_partkey integer     not null,
    s_name     char(25)    not null,
    s_address  varchar(40) not null
);
create index on ev.q20_result (s_suppkey, ps_partkey);

create or replace function ev.lineitem_insert() returns trigger
    language plpgsql
as
$$
declare
    tuple                ev.q20_tmp;
    found_tuple          ev.q20_tmp;
    canadaforestsupplier ev.q20_canadaforestsuppliers;
    exists_old           boolean := false;
    new_availqty         numeric;
begin
    for tuple in (select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as sum_quantity
                  from lineitem_transition
                  where l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1995-01-01'
                  group by l_partkey, l_suppkey)
    loop
        select *
        into found_tuple
        from ev.q20_tmp
        where ev.q20_tmp.l_partkey = tuple.l_partkey
          and ev.q20_tmp.l_suppkey = tuple.l_suppkey;

        -- insert grouped tuple and save new and old available quantity for the given supplier and part
        if not found then
            new_availqty := tuple.sum_quantity;

            insert into ev.q20_tmp
            select tuple.*;
        else
            new_availqty := tuple.sum_quantity + found_tuple.sum_quantity;
            exists_old := true;

            update ev.q20_tmp
            set sum_quantity = tuple.sum_quantity + found_tuple.sum_quantity
            where ev.q20_tmp.l_partkey = tuple.l_partkey
              and ev.q20_tmp.l_suppkey = tuple.l_suppkey;
        end if;

        -- get canadian supplier for a 'forest'-like part for the given suppkey and partkey
        select *
        into canadaforestsupplier
        from ev.q20_canadaforestsuppliers
        where s_suppkey = tuple.l_suppkey
          and ps_partkey = tuple.l_partkey;

        -- supplier wasn't in the result before but now should be
        -- availqty grows monotic therefore if the same supplier was processed before we do not need to insert him now
        if found and not exists_old and new_availqty < canadaforestsupplier.ps_availqty then
            insert into ev.q20_result
            values (canadaforestsupplier.s_suppkey, canadaforestsupplier.ps_partkey, canadaforestsupplier.s_name,
                    canadaforestsupplier.s_address);

            -- supplier is in the result but needs to be removed
        elsif found and exists_old and new_availqty >= canadaforestsupplier.ps_availqty and
              found_tuple.sum_quantity < canadaforestsupplier.ps_availqty then
            delete
            from ev.q20_result
            where ev.q20_result.s_suppkey = canadaforestsupplier.s_suppkey
              and ev.q20_result.ps_partkey = canadaforestsupplier.s_suppkey;
        end if;

        exists_old := false;
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
