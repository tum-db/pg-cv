create function bv.q15_query()
    returns table (
        like bv.q15_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from bv.q15_result;
end
$$;