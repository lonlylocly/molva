#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (разные части речи)
S -> Noun<no_hom> interp (SimpleFact.Noun);

S -> Adj<no_hom> interp (SimpleFact.Noun);

S -> Verb<no_hom> interp (SimpleFact.Noun);

S -> Adv<no_hom> interp (SimpleFact.Noun);

S -> Participle<no_hom> interp (SimpleFact.Noun);

S -> Word<gram="persn"> interp (SimpleFact.Noun);

S -> Word<gram="famn"> interp (SimpleFact.Noun);

// омонимы только для согласованных существительных
S -> Adj<gnc-agr[1]> interp (SimpleFact.Noun) Noun<gnc-agr[1]> interp (SimpleFact.Noun);

S -> Noun<gnc-agr[1]> interp (SimpleFact.Noun) Adj<gnc-agr[1]> interp (SimpleFact.Noun);

S -> Noun<gnc-agr[2]> interp (SimpleFact.Noun) Verb<gnc-agr[2]> interp (SimpleFact.Noun);

S -> Verb<gnc-agr[2]> interp (SimpleFact.Noun) Noun<gnc-agr[2]> interp (SimpleFact.Noun) ;
