create function fv.q15_query()
    returns table (
        like fv.q15_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from fv.q15_result;
end
$$;
