create table if not exists fv.q6_result (
    revenue numeric
);

create or replace function fv.lineitem_insert(entries stream_lineitem[]) returns void
    language plpgsql
as
$$
declare
    tuple       fv.q6_result;
    found_tuple fv.q6_result;
begin
    for tuple in (select sum(l_extendedprice * l_discount) as revenue
                  from unnest(entries)
                  where l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1995-01-01'
                    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                    and l_quantity < 24)
    loop
        select *
        into found_tuple
        from fv.q6_result;

        if not found then
            insert into fv.q6_result
            select tuple.*;
        else
            update fv.q6_result
            set revenue = coalesce(coalesce(tuple.revenue, 0) + found_tuple.revenue,
                                   tuple.revenue + coalesce(found_tuple.revenue, 0));
        end if;
    end loop;
end
$$;
