create function ev.q6_query()
    returns table (
        like ev.q6_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from ev.q6_result;
end
$$;