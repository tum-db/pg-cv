create function fv.q6_query()
    returns table (
        like fv.q6_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from fv.q6_result;
end
$$;