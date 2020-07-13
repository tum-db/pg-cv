create function dv.${query}_refresh() returns void
    language plpgsql
as
$$
begin
    refresh materialized view dv.${query};
end
$$;