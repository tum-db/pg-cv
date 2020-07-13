create function ev.q1_query()
    returns table (
        l_returnflag   char(1),
        l_linestatus   char(1),
        sum_qty        numeric,
        sum_base_price numeric,
        sum_disc_price numeric,
        sum_charge     numeric,
        avg_qty        numeric,
        avg_price      numeric,
        avg_disc       numeric,
        count_order    bigint
    )
    language plpgsql
as
$$
begin
    return query select t.l_returnflag                          as l_returnflag,
                        t.l_linestatus                          as l_returnflag,
                        t.sum_qty                               as sum_qty,
                        t.sum_base_price                        as sum_base_price,
                        t.sum_disc_price                        as sum_disc_price,
                        t.sum_charge                            as sum_charge,
                        t.sum_qty * 1.00 / t.count_order        as avg_qty,
                        t.sum_base_price * 1.00 / t.count_order as avg_price,
                        t.sum_disc * 1.00 / t.count_order       as avg_disc,
                        t.count_order                           as count_order
                 from ev.q1_result as t;
end
$$;