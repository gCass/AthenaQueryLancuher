# AthenaQueryLancuher

## Introduzione
Lo script è predisposto in modo da distruggere e ricreare una serie di tabelle su Athena. 
Lo schema di riferimento nello script è: l3_rep_research. Le funzioni sono predisposte per poter selezionare lo schema passandolo come parametro, il codice
ora non lo fa tuttavia (possibile futura modifica).
Esistono due versioni dello script:
1) Sincrona: le query vengono eseguite in sequenza come specificate nel file di configurazione. Corrisponde al branch master.
2) Asincrona: le query vengono eseguite tutte in parallelo

Le librerie da installare sono:
- boto3

## Funzionamento
Lo script è composto dai seguenti step:
1) Lettura di un file di configurazione presente nel percorso conf/conf.ini. In questo file sono specificati 2 parametri.  
   - <b>file_with_list_of_query_to_launch</b>: specifica il percorso di un file in cui vengono inseriti percorsi assoluti delle query da eseguire.
      Per comodità si consiglia di inserirlo sempre nella cartella <b>conf</b>.
   - <b>launch_date</b>: specifica la data di esecuzione in cui vengono eseguite le query nel formato YYYY-MM-DD.
2) Preparazione delle query: ricava dal file di configurazione l'elenco delle query da eseguire.  
   Legge la query e fa le seguenti sostituzioni nel codice che memorizza lo script:
    - Sostituisce i "now()" con la launch_date
    - Sostituisce "l3_rep_health.he_polizza" con "l3_rep_health_temp.he_polizza_temp"
3) Se una tabella con stesso nome del file contenente la query esiste, la droppa
4) Esegue la query creando la tabella (col nome specificato interno del codice sql)

Se in modalità asincrona i punti 3 e 4 vengono eseguiti in parallelo per ogni query. Altrimenti vengono ripetuti per ogni query in sequenza.

## Esecuzione
Il codice per ora funziona solo su Windows.  
```
python main.py
```
