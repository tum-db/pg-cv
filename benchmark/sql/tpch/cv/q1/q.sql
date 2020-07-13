with grouped_lineitem as (select l_returnflag,
                                 l_linestatus,
                                 sum(l_quantity)                                       as sum_qty,
                                 sum(l_extendedprice)                                  as sum_base_price,
                                 sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
                                 sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                                 sum(l_quantity)                                       as avg_qty,
                                 sum(l_extendedprice)                                  as avg_price,
                                 sum(l_discount)                                       as avg_disc,
                                 count(*)                                              as count_order
                          from stream_lineitem
                          where l_shipdate <= date '1998-12-01' - interval '90' day
                          group by l_returnflag, l_linestatus)

select l_returnflag,
       l_linestatus,
       sum_qty                        as sum_qty,
       sum_base_price                 as sum_base_price,
       sum_disc_price                 as sum_disc_price,
       sum_charge                     as sum_charge,
       avg_qty * 1.00 / count_order   as avg_qty,
       avg_price * 1.00 / count_order as avg_price,
       avg_disc * 1.00 / count_order  as avg_disc,
       count_order                    as count_order
from grouped_lineitem;
