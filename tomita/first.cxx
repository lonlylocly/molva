#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (разные части речи)
S -> Noun<no_hom> interp (SimpleFact.Noun);

// омонимы только для согласованных существительных
S -> Adj<gnc-agr[1]> Noun<gnc-agr[1]> interp (AdjNounFact.Noun);

S -> Noun<gnc-agr[1]> interp (AdjNounFact.Noun) Adj<gnc-agr[1]> ;

S -> Noun<rt, sp-agr[2]> interp (VerbNounFact.Noun) Verb<sp-agr[2]>;

S -> Verb<sp-agr[2]> Noun<rt, sp-agr[2]> interp (VerbNounFact.Noun) ;
