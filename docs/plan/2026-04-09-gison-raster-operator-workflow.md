# GISON Raster Operator Workflow

Stan na `2026-04-09`.

Ten runbook opisuje, jak operacyjnie prowadzić trudne JST z `facade WFS` lub `WMS-only` do produkcyjnego `gison_raster`.

## 1. Cel

`gison_raster` służy do JST, które:
- mają publiczny `wykazplanow`,
- publikują `WMS`, `GeoTIFF` albo legendę,
- ale nie publikują bezpośrednio wektorowych stref przeznaczenia w klasycznym `WFS`.

## 2. Narzędzia

Najważniejszy entrypoint:

```bash
cd /Users/michalszarek/worksapace/terrazoning/backend
uv run python run_wfs_sync.py
```

Kluczowe tryby:

```bash
uv run python run_wfs_sync.py --probe-gison-index '<wykazplanow_url>' --teryt <TERYT>
uv run python run_wfs_sync.py --probe-gison-wms '<wms_url>' --probe-gison-layer '<layer>' --probe-gison-legend-url '<legend_url>' --probe-gison-bbox 'xmin,ymin,xmax,ymax'
uv run python run_wfs_sync.py --teryt <TERYT>
```

## 3. Source states

`run_wfs_sync.py` zwraca teraz jawny `source_state`.
Przy planach pokrywających nasze działki probe pokazuje też stan assetów:
- `wms_health`
- `geotiff_health`

Znaczenie stanów:

- `ready`
  - źródło ma wystarczającą semantykę, żeby przejść do produkcyjnego ingestu
- `source_discovered`
  - źródło istnieje i wygląda obiecująco, ale nie jest jeszcze gotowe do bezpiecznego ingestu
- `bbox_axis_suspect`
  - parcelle wpadają w plan dopiero po zamianie osi bbox; źródło wymaga `swap_bbox_axes`
- `legend_missing_semantics`
  - WMS działa, ale parser legendy nie wyciąga jeszcze użytecznych klas
- `manual_override_required`
  - plan jest dobrym kandydatem, ale potrzebuje ręcznego override’u legendy

Interpretacja asset health:
- `wms_health = ok`
  - WMS odpowiada poprawnym `GetCapabilities`
- `wms_health = dead`
  - URL z `wykazplanow` nie jest dziś używalnym endpointem WMS
- `geotiff_health = ok`
  - publiczny GeoTIFF jest dostępny i może być dalszym źródłem klasyfikacji
- `geotiff_health = dead`
  - asset TIFF nie nadaje się dziś do pracy

## 4. Standardowy workflow

### Krok A: wybór planu

Uruchom:

```bash
uv run python run_wfs_sync.py --probe-gison-index '<wykazplanow_url>' --teryt <TERYT>
```

Szukaj planów, które mają:
- `parcel_match_count > 0`
- albo `bbox_axes_suspect = true`

Priorytet:
1. `ready`
2. `bbox_axis_suspect`
3. `manual_override_required`
4. `source_discovered`

### Krok B: próba WMS

Uruchom:

```bash
uv run python run_wfs_sync.py \
  --probe-gison-wms '<wms_url>' \
  --probe-gison-layer '<layer>' \
  --probe-gison-legend-url '<legend_url>' \
  --probe-gison-bbox 'xmin,ymin,xmax,ymax'
```

Interpretacja:
- `ready` -> można promować do registry
- `legend_missing_semantics` -> trzeba dodać parser/override legendy
- `manual_override_required` -> legenda działa częściowo, ale trzeba dopisać jawne mapowanie

### Krok C: promotion do registry

Do `WFS_REGISTRY` dodaj wpis `source_kind='gison_raster'`, gdy:
- plan pokrywa realne działki,
- WMS odpowiada stabilnie,
- legenda jest klasyfikowalna,
- mamy wystarczająco bezpieczne `designation` dla ingestu

### Krok D: sync i walidacja

Uruchom:

```bash
uv run python run_wfs_sync.py --teryt <TERYT>
```

Potem:

```bash
uv run python -m app.services.delta_engine
```

lub celowany delta run dla działek z danego TERYT-u.

## 5. Aktualne przykłady

### Jabłonka / `1211052`

Status:
- produkcyjnie działa przez `gison_raster`
- wymaga `swap_bbox_axes`
- używa ręcznego override’u legendy dla planu `002`

### Jeleśnia / `2417042`

Status:
- plan `009` jest już produkcyjnie podpięty
- index probe: `ready`
- używa ręcznego override'u legendy dla `009_legenda.pdf`
- live sync zapisuje strefy przez `gison_raster`
- delta liczy już przecięcia dla działki `241704201.2724/11`

### Andrychów / `1218014`

Status:
- plan `06` jest już produkcyjnie podpięty przez `GeoTIFF-backed gison_raster`
- plan pokrywa 4 obecne działki po zamianie osi bbox
- asset health:
  - `wms_health = dead`
  - `geotiff_health = ok`
- live sync zapisuje strefy przez `gison_raster`
- delta wygenerowała nowe leady dla działek `2998/61`, `2998/63`, `2998/70`, `2998/72`

Praktyczna zasada:
- gdy `wms_health = dead`, ale `geotiff_health = ok`, można promować źródło do registry
  przez `geotiff_url + sample_bbox_2180 + swap_bbox_axes`, bez czekania na działający WMS

## 6. Zasada bezpieczeństwa

Nie promujemy źródła do registry tylko dlatego, że:
- ma publiczny `WMS`,
- ma `legend_url`,
- albo `GetFeatureInfo` zwraca metadane APP.

Promocja jest bezpieczna dopiero wtedy, gdy umiemy przypisać kolor/symbol do realnego `przeznaczenie` z akceptowalną pewnością.
