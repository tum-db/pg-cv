create table if not exists fv.q15_tmp (
    supplier_no   integer not null,
    total_revenue numeric
);
create index on fv.q15_tmp (supplier_no);

create table if not exists fv.q15_result (
    s_suppkey     integer     not null,
    s_name        char(25)    not null,
    s_address     varchar(40) not null,
    s_phone       char(15)    not null,
    total_revenue numeric
);
create index on fv.q15_result (s_suppkey);

create or replace function fv.lineitem_insert(entries stream_lineitem[]) returns void
    language plpgsql
as
$$
declare
    tuple       fv.q15_tmp;
    found_tuple fv.q15_tmp;
    revenue     numeric;
    max_revenue numeric;
begin
    for tuple in (select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
                  from unnest(entries)
                  where l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-04-01'
                  group by l_suppkey)
    loop
        select *
        into found_tuple
        from fv.q15_tmp
        where fv.q15_tmp.supplier_no = tuple.supplier_no;

        -- insert tuple in tmp relation and keep revenue to determine whether to insert tuple into result
        if not found then
            revenue := tuple.total_revenue;

            insert into fv.q15_tmp
            select tuple.*;
        else
            revenue := tuple.total_revenue + found_tuple.total_revenue;

            update fv.q15_tmp
            set total_revenue = revenue
            where fv.q15_tmp.supplier_no = tuple.supplier_no;
        end if;

        -- get previous max revenue (max revenue in result)
        max_revenue := coalesce((select max(total_revenue)
                                 from fv.q15_result), 0);

        -- truncate result if revenue of new tuple is larger
        if revenue > max_revenue then truncate fv.q15_result; end if;

        -- insert new tuple if revenue is larger or equal
        if revenue >= max_revenue then
            insert into fv.q15_result
            select s_suppkey, s_name, s_address, s_phone, revenue
            from supplier
            where s_suppkey = tuple.supplier_no
              and not exists(select *
                             from fv.q15_result
                             where fv.q15_result.s_suppkey = supplier.s_suppkey);
        end if;
    end loop;
end
$$;
