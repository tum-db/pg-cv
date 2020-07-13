create function cv.${query}_query()
    returns table (
        like cv.${query}
    )
    language plpgsql
as
$$
begin
    return query select *
                 from cv.${query};
end
$$;