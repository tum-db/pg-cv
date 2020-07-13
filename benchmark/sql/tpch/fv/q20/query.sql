create function fv.q20_query()
    returns table (
        s_name    char(25),
        s_address varchar(40)
    )
    language plpgsql
as
$$
begin
    return query select r.s_name, r.s_address
                 from fv.q20_result as r
                 group by r.s_suppkey, r.s_name, r.s_address;
end
$$;
