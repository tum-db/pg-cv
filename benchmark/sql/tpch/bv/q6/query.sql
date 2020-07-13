create function bv.q6_query()
    returns table (
        like bv.q6_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from bv.q6_result;
end
$$;