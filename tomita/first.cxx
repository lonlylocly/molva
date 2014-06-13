#encoding "utf-8"    // сообщаем парсеру о том, в какой кодировке написана грамматика

// не разрешаем омонимы (любая часть речи)

S -> Word<no_hom> interp (SimpleFact.Noun);

// или имя-фамилия
S -> Word<gram="persn"> interp (SimpleFact.Noun);

S -> Word<gram="famn"> interp (SimpleFact.Noun);

// или согласовано с предыдущим словом
S -> Word<gnc-agr[1]> interp (SimpleFact.Noun) Word<gnc-agr[1], rt> interp (SimpleFact.Noun);


