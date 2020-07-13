create function bv.q3_query()
    returns table (
        like bv.q3_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from bv.q3_result;
end
$$;