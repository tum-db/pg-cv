create function ev.q15_query()
    returns table (
        like ev.q15_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from ev.q15_result;
end
$$;