do
$$
    begin
        perform cv.createcontinuousviewfromfile('${query}', '/benchmark/sql/tpch/cv/${query}/q.sql');
    end
$$;
