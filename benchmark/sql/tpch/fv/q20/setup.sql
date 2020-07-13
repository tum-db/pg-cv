create table fv.q20_canadaforestsuppliers as (select s_suppkey, s_name, s_address, ps_partkey, ps_availqty
                                              from nation, supplier, partsupp, part
                                              where n_name = 'CANADA'
                                                and n_nationkey = s_nationkey
                                                and s_suppkey = ps_suppkey
                                                and ps_partkey = p_partkey
                                                and p_name like 'forest%');
create index on fv.q20_canadaforestsuppliers (s_suppkey, ps_partkey);

create table fv.q20_tmp (
    l_partkey    integer not null,
    l_suppkey    integer not null,
    sum_quantity numeric
);
create index on fv.q20_tmp (l_partkey, l_suppkey);

create table fv.q20_result (
    s_suppkey  integer     not null,
    ps_partkey integer     not null,
    s_name     char(25)    not null,
    s_address  varchar(40) not null
);
create index on fv.q20_result (s_suppkey, ps_partkey);

create or replace function fv.lineitem_insert(entries stream_lineitem[]) returns void
    language plpgsql
as
$$
declare
    tuple                fv.q20_tmp;
    found_tuple          fv.q20_tmp;
    canadaforestsupplier fv.q20_canadaforestsuppliers;
    exists_old           boolean := false;
    new_availqty         numeric;
begin
    for tuple in (select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as sum_quantity
                  from unnest(entries)
                  where l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1995-01-01'
                  group by l_partkey, l_suppkey)
    loop
        select *
        into found_tuple
        from fv.q20_tmp
        where fv.q20_tmp.l_partkey = tuple.l_partkey
          and fv.q20_tmp.l_suppkey = tuple.l_suppkey;

        -- insert grouped tuple and save new and old available quantity for the given supplier and part
        if not found then
            new_availqty := tuple.sum_quantity;

            insert into fv.q20_tmp
            select tuple.*;
        else
            new_availqty := tuple.sum_quantity + found_tuple.sum_quantity;
            exists_old := true;

            update fv.q20_tmp
            set sum_quantity = tuple.sum_quantity + found_tuple.sum_quantity
            where fv.q20_tmp.l_partkey = tuple.l_partkey
              and fv.q20_tmp.l_suppkey = tuple.l_suppkey;
        end if;

        -- get canadian supplier for a 'forest'-like part for the given suppkey and partkey
        select *
        into canadaforestsupplier
        from fv.q20_canadaforestsuppliers
        where s_suppkey = tuple.l_suppkey
          and ps_partkey = tuple.l_partkey;

        -- supplier wasn't in the result before but now should be
        -- availqty grows monotic therefore if the same supplier was processed before we do not need to insert him now
        if found and not exists_old and new_availqty < canadaforestsupplier.ps_availqty then
            insert into fv.q20_result
            values (canadaforestsupplier.s_suppkey, canadaforestsupplier.ps_partkey, canadaforestsupplier.s_name,
                    canadaforestsupplier.s_address);

            -- supplier is in the result but needs to be removed
        elsif found and exists_old and new_availqty >= canadaforestsupplier.ps_availqty and
              found_tuple.sum_quantity < canadaforestsupplier.ps_availqty then
            delete
            from fv.q20_result
            where fv.q20_result.s_suppkey = canadaforestsupplier.s_suppkey
              and fv.q20_result.ps_partkey = canadaforestsupplier.s_suppkey;
        end if;

        exists_old := false;
    end loop;
end
$$;
