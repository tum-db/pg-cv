create table if not exists fv.q1_result (
    l_returnflag   char(1) not null,
    l_linestatus   char(1) not null,
    sum_qty        numeric,
    sum_base_price numeric,
    sum_disc_price numeric,
    sum_charge     numeric,
    sum_disc       numeric,
    count_order    bigint
);
create index on fv.q1_result (l_returnflag, l_linestatus);

create or replace function fv.lineitem_insert(entries stream_lineitem[]) returns void
    language plpgsql
as
$$
declare
    tuple       fv.q1_result;
    found_tuple fv.q1_result;
begin
    for tuple in select l_returnflag,
                        l_linestatus,
                        sum(l_quantity)                                       as sum_qty,
                        sum(l_extendedprice)                                  as sum_base_price,
                        sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
                        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                        sum(l_discount)                                       as sum_disc,
                        count(*)                                              as count_order
                 from unnest(entries)
                 where l_shipdate <= date '1998-12-01' - interval '90' day
                 group by l_returnflag, l_linestatus
    loop
        select *
        into found_tuple
        from fv.q1_result
        where fv.q1_result.l_returnflag = tuple.l_returnflag
          and fv.q1_result.l_linestatus = tuple.l_linestatus;

        if not found then
            insert into fv.q1_result
            select tuple.*;
        else
            update fv.q1_result
            set sum_qty        = tuple.sum_qty + found_tuple.sum_qty,
                sum_base_price = tuple.sum_base_price + found_tuple.sum_base_price,
                sum_disc_price = tuple.sum_disc_price + found_tuple.sum_disc_price,
                sum_charge     = tuple.sum_charge + found_tuple.sum_charge,
                sum_disc       = tuple.sum_disc + found_tuple.sum_disc,
                count_order    = tuple.count_order + found_tuple.count_order
            where fv.q1_result.l_returnflag = tuple.l_returnflag
              and fv.q1_result.l_linestatus = tuple.l_linestatus;
        end if;
    end loop;
end
$$;
