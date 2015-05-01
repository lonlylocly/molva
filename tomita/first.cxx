#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (разные части речи)

Part -> Noun<no_hom> | Word<gram="persn"> | Word<gram="famn"> | Word<gram="geo">; 

NounWithPrep -> Part interp (SimpleFact.Noun) | Prep interp (SimpleFact.Prep) Part interp (SimpleFact.Noun);

OtherNoHomsGroup -> Adv<no_hom> | Adj<no_hom> | Verb<no_hom>; 

OtherNoHoms -> OtherNoHomsGroup interp (SimpleFact.Noun);

S -> NounWithPrep;

S -> OtherNoHoms;

// омонимы только для согласованных существительных
AlignedPair -> Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg") Noun<gnc-agr[1]> interp (SimpleFact.Noun);

AlignedPair -> Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg") Prep interp (SimpleFact.Prep) Noun<gnc-agr[1]> interp (SimpleFact.Noun);

AlignedPair -> Noun<gnc-agr[1]> interp (SimpleFact.Noun) Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg");

AlignedPair -> Prep interp (SimpleFact.Prep) Noun<gnc-agr[1]> interp (SimpleFact.Noun) Adj<gnc-agr[1]> interp (SimpleFact.Noun::norm="m,sg");

AlignedPair -> Noun<gnc-agr[2]> interp (SimpleFact.Noun) Verb<gnc-agr[2]> interp (SimpleFact.Noun);

AlignedPair -> Prep interp (SimpleFact.Prep) Noun<gnc-agr[2]> interp (SimpleFact.Noun) Verb<gnc-agr[2]> interp (SimpleFact.Noun);

AlignedPair -> Verb<gnc-agr[2]> interp (SimpleFact.Noun) Noun<gnc-agr[2]> interp (SimpleFact.Noun) ;

AlignedPair -> Verb<gnc-agr[2]> interp (SimpleFact.Noun) Prep interp (SimpleFact.Prep)  Noun<gnc-agr[2]> interp (SimpleFact.Noun) ;

S -> AlignedPair;

// parse hashtags
HashTag -> AnyWord<wff=/(_|[A-Za-zА-Яа-я0-9])+/>;

S -> '#' interp (SimpleFact.IsHashTag=true) HashTag interp (SimpleFact.Noun);

// Numericals

Numeric -> AnyWord<wff=/[0-9]+/>;

S -> Numeric interp (SimpleFact.Noun; SimpleFact.IsNumber=true);

// имена собственные
WordSepPart -> SimConjAnd | Hyphen | Comma ;

NounOrLike -> NounWithPrep | OtherNoHoms | AlignedPair | Prep;

WordSep -> NounOrLike | NounOrLike WordSepPart;

// First capital, not number, others not capitals
WordCapFirst -> Word<h-reg1,~h-reg2,wff=/[^0-9].+/>;

GeneralName -> WordCapFirst interp (SimpleFact.Noun; SimpleFact.IsPersonName=true);

S -> WordSep GeneralName + ;

