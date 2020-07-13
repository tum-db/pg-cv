with grouped_lineitem as (select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as sum_quantity
                          from stream_lineitem
                          where l_shipdate >= date '1994-01-01'
                            and l_shipdate < date '1995-01-01'
                          group by l_partkey, l_suppkey),
     forestpartsupp as (select ps_suppkey, ps_partkey, ps_availqty
                        from partsupp, part
                        where p_name like 'forest%'
                          and ps_partkey = p_partkey),
     canadasupp as (select s_suppkey, s_name, s_address
                    from supplier, nation
                    where s_nationkey = n_nationkey
                      and n_name = 'CANADA')

select s_name, s_address
from canadasupp
where s_suppkey in (
    select ps_suppkey from forestpartsupp
    where
            ps_availqty > (
            select sum_quantity
            from grouped_lineitem
            where
                    l_suppkey = ps_suppkey and
                    l_partkey = ps_partkey
        )
)
