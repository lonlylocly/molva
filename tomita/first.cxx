#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (разные части речи)

Part -> Noun<no_hom> | Word<gram="persn"> | Word<gram="famn"> | Word<gram="geo">; 

NounWithPrep -> Part interp (SimpleFact.Noun) | Prep interp (SimpleFact.Prep) Part interp (SimpleFact.Noun);

OtherNoHoms -> Adv<no_hom> | Adj<no_hom> | Verb<no_hom>; 

S -> NounWithPrep | OtherNoHoms interp (SimpleFact.Noun);


// омонимы только для согласованных существительных
S -> Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg") Noun<gnc-agr[1]> interp (SimpleFact.Noun);

S -> Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg") Prep interp (SimpleFact.Prep) Noun<gnc-agr[1]> interp (SimpleFact.Noun);

S -> Noun<gnc-agr[1]> interp (SimpleFact.Noun) Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg");

S -> Prep interp (SimpleFact.Prep) Noun<gnc-agr[1]> interp (SimpleFact.Noun) Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg");

S -> Noun<gnc-agr[2]> interp (SimpleFact.Noun) Verb<gnc-agr[2]> interp (SimpleFact.Noun);

S -> Prep interp (SimpleFact.Prep) Noun<gnc-agr[2]> interp (SimpleFact.Noun) Verb<gnc-agr[2]> interp (SimpleFact.Noun);

S -> Verb<gnc-agr[2]> interp (SimpleFact.Noun) Noun<gnc-agr[2]> interp (SimpleFact.Noun) ;

S -> Verb<gnc-agr[2]> interp (SimpleFact.Noun) Prep interp (SimpleFact.Prep)  Noun<gnc-agr[2]> interp (SimpleFact.Noun) ;

// parse hashtags
HashTag -> AnyWord<wff=/(_|[A-Za-zА-Яа-я0-9])+/>;

S -> '#' interp (SimpleFact.IsHashTag=true) HashTag interp (SimpleFact.Noun);

// имена собственные
WordSepPart -> SimConjAnd | Hyphen | Comma | Colon;

SimpleWord -> Word {weight = 0.5}; 

NohomOrWord -> SimpleWord | NounWithPrep | OtherNoHoms interp (SimpleFact.Noun)  ;

WordSep -> NohomOrWord | NohomOrWord WordSepPart;

// First capital, not number, others not capitals
WordCapFirst -> Word<h-reg1,~h-reg2,wff=/[^0-9].+/>;

GeneralName -> WordCapFirst interp (SimpleFact.Noun; SimpleFact.IsPersonName=true);

S -> WordSep GeneralName + ;

