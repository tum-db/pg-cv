create function dv.${query}_query()
    returns table (
        like dv.${query}
    )
    language plpgsql
as
$$
begin
    return query select *
                 from dv.${query};
end
$$;
