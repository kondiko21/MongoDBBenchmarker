# MongoDB CAP/PACELC benchmark

Orkiestrator służy do badania wpływu ustawień spójności MongoDB na wydajność i świeżość odczytów. Eksperymenty są wykonywane dla topologii Replica Set złożonych z 3 lub 5 węzłów oraz dla wybranych profili opóźnienia i jittera sieci replikacyjnej.

## Badane zmienne

- topologia replica setu: 3 albo 5 pełnych węzłów;
- `writeConcern`: `1`, `majority`;
- `readConcern`: `local`, `majority`, `linearizable`;
- `readPreference`: `primary`, `secondary`;
- kontrolowane latency i jitter pomiędzy węzłami przez `tc netem`;
- współbieżność klienta.

## Eksperymenty

Główny eksperyment `performance` wykonuje w każdym przebiegu kontrolowaną mieszaninę operacji:

- 50% odczytów;
- 25% insertów;
- 25% aktualizacji.

Każdy przebieg używa jednej kompletnej konfiguracji z iloczynu `writeConcern`, `readConcern`,
`readPreference`, topologii, profilu sieciowego i współbieżności. Wynik zawiera osobne metryki
`read`, `insert`, `update` oraz `overall`, dzięki czemu wpływ konfiguracji jest badany dla wszystkich
operacji w tych samych warunkach.

Eksperyment `freshness` jest uruchamiany osobno. Jeden writer nadaje każdemu rejestrowi 
rosnące wersje, a równolegli czytelnicy mierzą widoczną wersję. Każdy zapis i odczyt tworzy surowy
rekord historii zawierający: identyfikator operacji, typ operacji, klienta, klucz, czas rozpoczęcia
i zakończenia mierzony monotonicznym zegarem, zapisaną albo odczytaną wersję, wynik i klasę błędu.

Benchmark nie wpisuje do historii gotowego `versionLag`. Niezależny skrypt offline rekonstruuje
dla każdego odczytu najnowszy udany zapis tego samego klucza zakończony nie później niż rozpoczęcie
odczytu. Odczyt jest nieaktualny wyłącznie wtedy, gdy jego `observedVersion` jest od tej wersji
mniejsza. Zapis nakładający się czasowo na odczyt nie jest używany jako punkt odniesienia.


## Uruchomienie

Wymagane są działający Docker Desktop i wolne porty 27017-27021.

Smoke test sprawdzający całą ścieżkę na małej macierzy (jego wyników nie należy mieszać z pomiarami):

```bash
./gradlew run --args="--config=config/smoke-experiment.yml"
```

Główny eksperyment wydajnościowy:

```bash
./gradlew run --args="--config=config/performance-experiment.yml"
```

Osobny eksperyment świeżości/spójności:

```bash
./gradlew run --args="--config=config/consistency-experiment.yml"
```

Eksperymenty uzupełniające (`maxStalenessSeconds`, Flow Control i chaining):

```bash
./gradlew run --args="--config=config/supplementary-experiment.yml"
```

Każde uruchomienie tworzy nowy katalog `results/.../experiment-YYYYMMDD-HHMMSS`. `summary.jsonl` zawiera metryki i pełne parametry przebiegu,
a pliki `*-history.jsonl` z eksperymentu spójności zawierają kompletną historię zapisów i odczytów.

Analiza historii świeżości:

```bash
python3 analysis/analyze_freshness.py results/consistency/experiment-YYYYMMDD-HHMMSS
```

Skrypt zwraca kod `0` tylko wtedy, gdy historia spełnia kryterium odbioru: opóźnione
`secondary + local` zawiera co najmniej jeden nieaktualny odczyt, odpowiadający scenariusz
`primary + local` nie zawiera żadnego, a wersje zapisów są ściśle rosnące. Dodatkowo historia nie
może zawierać błędów operacji, powtórzonych identyfikatorów ani niepoprawnych przedziałów czasu.
Brak wymaganego scenariusza również oznacza niepowodzenie, dzięki czemu nie da się omyłkowo uznać
niepełnego eksperymentu za poprawny.
Do samego przeglądania częściowych wyników służy opcja `--report-only`.

## Trzy zestawy parametrów

1. `performance-parameters.yml` - pełna macierz wydajności dla read/insert/update.
2. `consistency-parameters.yml` - osobna macierz dla wersjonowanych rejestrów i świeżości.
3. `supplementary-parameters.yml` - pełna macierz główna rozszerzona o listy
   `maxStalenessSeconds`, `enableFlowControl`, `flowControlTargetLagSeconds` i `chainingAllowed`.
