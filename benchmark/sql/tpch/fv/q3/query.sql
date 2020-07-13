create function fv.q3_query()
    returns table (
        like fv.q3_result
    )
    language plpgsql
as
$$
begin
    return query select *
                 from fv.q3_result;
end
$$;
