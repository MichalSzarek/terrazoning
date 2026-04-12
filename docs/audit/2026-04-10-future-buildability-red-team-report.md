# Future Buildability Red-Team Report

- corpus_path: `data/future_buildability_validation_corpus.json`
- matched_rows: `30`

## Forest / Green False Positives
- `121701111.106/2` | gmina `1217011` | band `None` | overall `48.00` | signal `28.00` | cheapness `20.00` | dominant `studium_zone: residential (001)`

## Cheap But Unjustified False Positives
- `240902501.2440` | gmina `2409025` | band `None` | overall `40.00` | signal `20.00` | cheapness `20.00` | dominant `planning_resolution: unknown (Studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy i miasta Koziegłowy)`
- `241608501.348` | gmina `2416085` | band `None` | overall `40.00` | signal `20.00` | cheapness `20.00` | dominant `planning_resolution: unknown (w sprawie uchwalenia zmiany Studium uwarunkowań i kierunków zagospodarowania przestrzennego Miasta i Gminy Szczekociny)`
- `120506204.262/3` | gmina `1205062` | band `None` | overall `20.00` | signal `0.00` | cheapness `20.00` | dominant `-`
- `246601161.15/59` | gmina `2466011` | band `None` | overall `25.00` | signal `5.00` | cheapness `20.00` | dominant `studium_zone: mixed_residential (Studium uwarunkowań i kierunków zagospodarowania przestrzennego miasta Gliwice)`
- `120507201.2695` | gmina `1205072` | band `None` | overall `20.00` | signal `0.00` | cheapness `20.00` | dominant `-`
- `121701111.101/2` | gmina `1217011` | band `None` | overall `20.00` | signal `0.00` | cheapness `20.00` | dominant `studium_zone: residential (001)`

## Preparatory Document Over-Weighting
- `241402101.708/1` | gmina `2414021` | band `None` | overall `50.00` | signal `30.00` | cheapness `20.00` | dominant `planning_resolution: unknown (w sprawie uchwalenia Studium uwarunkowań i kierunków zagospodarowania przestrzennego Miasta Imielin)`
- `120106510.1443/4` | gmina `1201065` | band `None` | overall `50.00` | signal `30.00` | cheapness `20.00` | dominant `planning_resolution: unknown (w sprawie uchwalenia studium uwarunkowań i kierunków zagospodarowania przestrzennego Gminy Nowy Wiśnicz)`
- `240404201.827` | gmina `2404042` | band `None` | overall `45.00` | signal `25.00` | cheapness `20.00` | dominant `planning_resolution: unknown (Studium uwarunkowań i kierunków zagospodarowania przestrzennego Gminy Kamienica Polska)`
- `121306202.277/12` | gmina `1213062` | band `None` | overall `50.00` | signal `30.00` | cheapness `20.00` | dominant `planning_resolution: unknown (POG 1213062)`
- `121809503.3827/1` | gmina `1218095` | band `None` | overall `50.00` | signal `30.00` | cheapness `20.00` | dominant `planning_resolution: unknown (w sprawie zmiany studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy Wadowice)`
- `240902501.2440` | gmina `2409025` | band `None` | overall `40.00` | signal `20.00` | cheapness `20.00` | dominant `planning_resolution: unknown (Studium uwarunkowań i kierunków zagospodarowania przestrzennego gminy i miasta Koziegłowy)`
- `241608501.348` | gmina `2416085` | band `None` | overall `40.00` | signal `20.00` | cheapness `20.00` | dominant `planning_resolution: unknown (w sprawie uchwalenia zmiany Studium uwarunkowań i kierunków zagospodarowania przestrzennego Miasta i Gminy Szczekociny)`

## Stale Or Invalid Source Promotion
- none flagged in the current corpus
