create function ev.q3_query()
    returns table (
        like ev.q3_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from ev.q3_result;
end
$$;