# Algorytm \- Busy w Dubaicach Świętokrzyskich

## Parametry

- n \- liczba mieszkańców (procesów)  
- B \- liczba busów (niezależnych sekcji krytycznych)  
- P \- pojemność każdego busa (jednostki towaru)  
- X \- maksymalny rozmiar zamówienia jednego mieszkańca (1 \<= zamówienie \<= X)

Każdy mieszkaniec to osobny proces. Każdy bus to niezależna uogólniona sekcja krytyczna o zmiennej pojemności. Algorytm oparty na algorytmie Lamporta.

---

## Struktury i zmienne

- clock \- zegar Lamporta  
- queue\[b\] \- kolejka żądań REQ dla busa b, posortowana po (ts, id), początkowo pusta  
- lastMsg\[j\] \- najnowszy timestamp wiadomości od procesu j, początkowo 0 dla każdego j  
- myBus \- numer busa wybranego w bieżącej rundzie  
- myOrder \- rozmiar bieżącego zamówienia (1 \<= myOrder \<= X)  
- myTS \- timestamp wysłanego REQ  
- arrivedBuf \- bufor na ARRIVED otrzymany przed spełnieniem W1+W2, początkowo null

---

## Wiadomości

Wszystkie wiadomości zawierają timestamp zgodny z regułami zegara Lamporta.

- REQ(ts, i, b, order) \- żądanie wejścia do busa b z zamówieniem rozmiaru order  
- ACK(ts, i) \- potwierdzenie żądania  
- ARRIVED(ts, driver, b, passengers\[\]) \- kierowca busa b wrócił z towarem; zawiera listę pasażerów; pełni rolę zbiorczego RELEASE

---

## Stany

Początkowym stanem procesu jest REST.

- REST \- nie ubiega się o miejsce w busie  
- WAIT \- wysłał REQ, czeka na spełnienie W1+W2  
- ONBUS \- potwierdzona obecność na busie, czeka na powrót kierowcy  
- DRIVING \- wyznaczony kierowca; monitoruje zapełnienie, następnie jedzie po towar

---

## Warunki wejścia i odjazdu

Mieszkaniec i potwierdza obecność na busie b gdy zachodzą jednocześnie:

**W1** \- load\_before(i, b) \+ myOrder \<= P, gdzie load\_before(i, b) to suma zamówień wszystkich REQ w queue\[b\] z priorytetem wyższym niż REQ\_i.

**W2** \- dla każdego j \!= i: lastMsg\[j\] \> myTS. Gwarantuje, że żaden przyszły REQ nie wskoczy przed REQ\_i w kolejce, a więc load\_before(i, b) jest wartością ostateczną.

Kierowcą zostaje pasażer z najniższym priorytetem \- ostatniREQ w queue\[b\] spośród pasażerów, czyli o największym (ts, id).

Bus odjeżdża gdy lista pasażerów jest ostateczna. Niech P1 oznacza ostatni REQ w queue\[myBus\] za ostatnim pasażerem (ostatni który się nie mieści).

Warunek (1) \- kolejka blokuje: load\_fitted \+ order(P1) \> P oraz dla każdego j nie będącego pasażerem: lastMsg\[j\] \>= ts(P1)

Drugi człon warunku (1) gwarantuje, że żaden proces j nie ma już w locie REQ z ts \< ts(P1), który mógłby wskoczyć przed P1 i zmieścić się w busie. Bez tego sprawdzenia kierowca mógłby odjechać widząc "nie mieści się P1(ts=20)", podczas gdy P2 dopiero co wysłał REQ(ts=15) który by się zmieścił.

Warunek (2) \- brak popytu: za ostatnim pasażerem nie ma żadnego REQ w queue\[myBus\] oraz każdy j \!= i ma REQ w jakimś queue\[b\] (żaden mieszkaniec nie jest w REST)

Warunek (2) zapobiega wiecznemu czekaniu gdy bus nie jest pełny \- jeśli wszyscy zadeklarowali już swój zamiar (są w WAIT/ONBUS/DRIVING dla jakiegoś busa), brak chętnych na ten bus jest informacją ostateczną.

---

## Opis szczegółowy algorytmu dla procesu i

REST \- stan początkowy. Proces przebywa w REST do czasu podjęcia decyzji o zamówieniu. Przejście do WAIT: wybierz myBus i myOrder, wyślij REQ(clock, i, myBus, myOrder) do wszystkich j \!= i, wstaw własne żądanie do queue\[myBus\], zapisz myTS \= clock, ustaw arrivedBuf \= null.

Reakcje na wiadomości w REST:

- REQ(ts, j, b, order): wstaw do queue\[b\], wyślij ACK  
- ACK: ignoruj  
- ARRIVED(ts, driver, b, passengers\[\]): usuń REQ pasażerów z queue\[b\]

---

WAIT \- oczekiwanie na wejście do busa. Po każdej wiadomości sprawdź W1 i W2.

Przejście WAIT \-\> DRIVING: gdy W1+W2 spełnione i `i` jest ostatnim w queue\[myBus\] spośród pasażerów.

Przejście WAIT \-\> ONBUS: gdy W1+W2 spełnione i `i` nie jest kierowcą. Po przejściu: jesli arrivedBuf \!= null \-\> natychmiast obsłuż buforowany ARRIVED.

Reakcje na wiadomości w WAIT:

- REQ(ts, j, b, order): wstaw do queue\[b\], wyślij ACK  
- ACK: ignoruj  
- ARRIVED(ts, driver, b, passengers\[\]): usuń REQ pasażerów z queue\[b\]; jeśli b \== myBus i i in passengers\[\] \-\> zapisz arrivedBuf \= wiadomość (kierowca wrócił przed spełnieniem W2 \- buforujemy)

ACK jest wysyłany natychmiast na każdy REQ niezależnie od priorytetu \- bezpieczeństwo zapewnia kolejka i W1+W2, nie wstrzymywanie ACK.

---

ONBUS \- potwierdzona obecność na busie; oczekiwanie na powrót kierowcy.

Przejście ONBUS \-\> REST: po otrzymaniu ARRIVED gdzie b \== myBus i `i` in passengers\[\] \-\> usuń REQ pasażerów z queue\[myBus\], przejdź do REST.  
Reakcje na wiadomości w ONBUS:

- REQ(ts, j, b, order): wstaw do queue\[b\], wyślij ACK  
- ACK: ignoruj  
- ARRIVED(ts, driver, b, passengers\[\]): jeśli b \== myBus i `i` in passengers\[\] \-\> przejście do REST; w przeciwnym razie usuń REQ pasażerów z queue\[b\]

---

DRIVING \- kierowca busa. Potwierdzona obecność (W1+W2) i ostatni w queue\[myBus\].

Faza 1 \- oczekiwanie na zapełnienie: po każdej wiadomości sprawdź warunek odjazdu. Gdy spełniony \-\> wyznacz listę pasażerów (wszystkie REQ w queue\[myBus\] spełniające W1 w kolejności kolejki) i wykonaj przejazd (operacja lokalna).

Faza 2 \- powrót: wyślij ARRIVED(clock, i, myBus, passengers\[\]) do wszystkich j \!= i, usuń REQ pasażerów z queue\[myBus\], przejdź do REST.

Reakcje na wiadomości w DRIVING:

- REQ(ts, j, b, order): wstaw do queue\[b\], wyślij ACK; jeśli b \== myBus \-\> ponownie sprawdź warunek odjazdu  
- ACK: ignoruj  
- ARRIVED(ts, driver, b, passengers\[\]): jeśli b \!= myBus \-\> usuń REQ pasażerów z queue\[b\]

