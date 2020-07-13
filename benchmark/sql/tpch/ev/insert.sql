create function ev.${query}_insert() returns void
    language plpgsql
as
$$
declare
    start_counter int := 1;
    end_counter   int := 0;
begin
    select max(counter)
    into end_counter
    from base_lineitem;

    while start_counter <= end_counter
    loop
        insert into ev.lineitem
        select l_orderkey,
               l_partkey,
               l_suppkey,
               l_linenumber,
               l_quantity,
               l_extendedprice,
               l_discount,
               l_tax,
               l_returnflag,
               l_linestatus,
               l_shipdate,
               l_commitdate,
               l_receiptdate,
               l_shipinstruct,
               l_shipmode,
               l_comment
        from base_lineitem
        where counter >= start_counter
          and counter < start_counter + ${batch_size};

        start_counter := start_counter + ${batch_size};
    end loop;
end
$$;