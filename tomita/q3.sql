        select  n1_md5, n2_md5, 1 / (similar * (p.sum_count + p2.sum_count)) as sim_k 
        from ns_view s   
        inner join post_sum_count p 
        on s.n1_md5 = p.noun_md5  
        inner join post_sum_count p2 
        on s.n2_md5 = p2.noun_md5
        where 
        s.n1_md5 < s.n2_md5
        and s.n1_md5 not in (127460222,194911016,348664285,522559933,1543116322,1928061337,2860766309,3235057763,3534832033,3759046491)  
        and s.n2_md5 not in (127460222,194911016,348664285,522559933,1543116322,1928061337,2860766309,3235057763,3534832033,3759046491)
        and similar > 0 
        order by sim_k asc ; 

