#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (разные части речи)
S -> Word interp (SimpleFact.Noun::norm="nom,sg");

// омонимы только для согласованных существительных
//S -> Adj<gnc-agr[1]> Noun<gnc-agr[1]> interp (AdjNounFact.Noun);
//
//S -> Noun<gnc-agr[1]> interp (AdjNounFact.Noun) Adj<gnc-agr[1]> ;
//
//S -> Noun<gnc-agr[2]> interp (VerbNounFact.Noun) Verb<gnc-agr[2]>;
//
//S -> Verb<gnc-agr[2]> Noun<gnc-agr[2]> interp (VerbNounFact.Noun) ;
