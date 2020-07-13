create function cv.${query}_refresh() returns void
    language plpgsql
as
$$
begin
    refresh materialized view cv.${query};
end
$$;