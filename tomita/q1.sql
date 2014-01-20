select noun1, noun2, (similar * (p.sum_count + p2.sum_count)) as sim_k 
from ns_view s  
inner join post_sum_count p 
on s.n1_md5 = p.noun_md5  
inner join post_sum_count p2 
on s.n2_md5 = p2.noun_md5
where 
s.n1_md5 < s.n2_md5
and noun1 not in ('а', 'нет', 'у', 'уж', 'ли', 'в', 'до', 'я', 'и', 'с') 
and noun2 not in ('а', 'нет', 'у', 'уж', 'ли', 'в', 'до', 'я', 'и', 'с') 
and sim_k > 0 
order by sim_k desc;
