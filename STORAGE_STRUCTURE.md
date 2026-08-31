# Storage Structure

Bagaimana `data/datasets/` ditata di disk, dan bagaimana ETL, database, dan API
saling sinkron soal path.

## Layout

```
data/
└── datasets/
    └── {dataset_id}_{slug_nama_dataset}/       # mis. 7_hakim_d1
        ├── metadata.json                       # ringkasan level-dataset (lihat di bawah)
        ├── raw/
        │   ├── sentinel1/
        │   │   └── {product_identifier}.SAFE/  # satu folder per scene Sentinel-1
        │   │       ├── {product_identifier}.SAFE.zip
        │   │       └── ..._VV.tif, ..._VH.tif
        │   ├── modis/                           # cache granule .hdf mentah (flat)
        │   └── gpm/                             # cache granule .nc4 mentah (flat)
        ├── bronze/
        │   └── sentinel1/
        │       └── {product_identifier}.SAFE/
        │           └── ..._VV_crop.tif, ..._VH_crop.tif
        ├── silver/
        │   ├── sentinel1/
        │   │   └── {product_identifier}.SAFE/
        │   │       ├── ..._VV_lee.tif, ..._VH_lee.tif
        │   │       └── metadata_qa.json         # metrik kualitas band scene ini
        │   ├── modis/
        │   │   └── {YYYYMMDD}/
        │   │       ├── modis_{date}_flood.tif
        │   │       ├── modis_{date}_ndvi.tif
        │   │       └── modis_{date}_ndwi.tif
        │   └── gpm/
        │       └── {YYYYMMDD}/
        │           └── gpm_rain_{24h,72h,7d}_{date}.tif
        ├── gold/                                 # COG analysis-ready, per source
        │   ├── sentinel1/{product_identifier}.SAFE/
        │   ├── modis/{YYYYMMDD}/
        │   └── gpm/{YYYYMMDD}/
        └── fusion/                               # lintas-source: TIDAK punya level source
            └── {YYYYMMDD}/
                ├── fusion_{date}.h5
                └── fusion_metadata.json
```

- `{dataset_id}_{slug_nama_dataset}` — primary key `datasets.dataset_id` digabung
  slug dari `datasets.name` (mis. dataset bernama "hakim d1" -> folder `7_hakim_d1`).
  Slug: karakter selain `A-Za-z0-9_.-` diganti `_` (fungsi `folder_manager.slugify`,
  sama persis dengan `pipeline_logger.py:_slug_filename` yang dipakai untuk nama
  file log per-dataset).
- Level `{source}` ada di **setiap tier kecuali `fusion`**. Tier fusion justru
  gabungan ketiga source, jadi memberinya satu folder source akan menyesatkan;
  `folder_manager` menolak permintaan source untuk tier itu alih-alih diam-diam
  menulis ke tempat yang salah.
- `{scene}` di dalam tiap source:
  - Sentinel-1: `product_identifier` scene tsb (`satellite_scenes.product_identifier`,
    sudah unik termasuk jam:menit:detik). Ini yang membuat dua slice S1 dari orbit
    pass yang sama di hari yang sama tidak saling menimpa — tanggal saja bukan
    kunci unik untuk Sentinel-1.
  - MODIS/GPM dan output fusion: tanggal akuisisi `YYYYMMDD` (produk harian,
    di-dedup per hari).
- `raw/modis/` dan `raw/gpm/` sengaja **flat**, bukan per-tanggal: satu granule GPM
  harian ikut dipakai window 72h/7d tanggal-tanggal berikutnya, jadi tidak bisa
  dimiliki satu folder tanggal saja. Isinya file granule, bukan folder scene.
- `bronze/` hanya punya `sentinel1/`: MODIS/GPM tidak lewat tahap crop terpisah —
  mereka langsung dari granule mentah di `raw/` ke produk harian di `silver/`.
- `_work/{scene}/` di level root dataset adalah folder kerja sementara (hasil
  kalibrasi radiometrik sebelum crop) — dihapus otomatis setelah tahap CROP
  selesai, bukan bagian dari tier resmi.

## Tier

| Tier   | Dihasilkan oleh                                     | Isi                                                                    |
|--------|-----------------------------------------------------|------------------------------------------------------------------------|
| raw    | `module1_download.py`, `module7`, `module8`          | ZIP asli + TIFF hasil ekstrak per band (S1); cache granule mentah (MODIS/GPM) |
| bronze | `module2_crop.py`                                    | TIFF S1 setelah dipotong ke bbox AOI                                     |
| silver | `module3_lee_filter.py`, `module7`, `module8`        | TIFF S1 setelah speckle filtering (Lee); GeoTIFF harian MODIS flood/NDVI/NDWI dan GPM rainfall |
| gold   | `module4_gold_export.py`                             | COG analysis-ready per band per source (overview + tiling internal)      |
| fusion | `module9_fusion.py`                                  | HDF5 multi-modal gabungan semua source + `fusion_metadata.json`          |

Pipeline penuh: DOWNLOAD (raw) → CROP (bronze) → LEE_FILTER (silver) →
QUALITY_ANALYTICS (metrik atas silver) → GOLD_EXPORT (gold) → FUSION (fusion).
Input MODIS/GPM disiapkan di tahap FUSION lewat
`module9_fusion.ensure_aux_inputs_for_date`, yang mengunduh ke silver lalu ikut
mengekspornya ke gold sebelum fusion membacanya.

`module6_analytics.py` menulis `metadata_qa.json` ke folder scene S1 di
`silver/sentinel1/{product_identifier}.SAFE/`, berisi metrik band VV/VH (quality
score, backscatter mean/std/min/max, speckle index, dsb). Metrik yang sama juga
disimpan di tabel `quality_metrics` — file JSON ini untuk siapa pun yang
menjelajah filesystem langsung tanpa akses database.

### Isi file fusion HDF5

Dataset di dalam `.h5` dikelompokkan per source, bukan datar:

```
/sentinel1/VV, /sentinel1/VH
/modis/FLOOD, /modis/NDVI, /modis/NDWI
/gpm/rainfall_24h, /gpm/rainfall_72h, /gpm/rainfall_7d
```

Semua layer sudah direproject ke grid Sentinel-1 scene tsb. `/modis/FLOOD`
bertipe uint8 dengan 255 = nodata (kelas banjir itu kategorikal, NaN tidak muat
di uint8); layer lain float32 dengan NaN sebagai nodata. Layer yang inputnya
tidak tersedia tetap ditulis, terisi penuh nodata — jadi bentuk file selalu sama
dan konsumen tidak perlu mengecek keberadaan dataset.

MODIS NDVI/NDWI dihitung dari surface reflectance MOD09GA:
`NDVI = (b02_NIR - b01_red) / (b02 + b01)`,
`NDWI = (b04_green - b02_NIR) / (b04 + b02)` (formulasi McFeeters — indeks air
permukaan, bukan NDWI Gao untuk kelembapan vegetasi). Indeksnya dihitung di grid
sinusoidal asli lalu direproject, bukan sebaliknya: meresample tiap band dulu
lalu membagi akan menggeser nilai indeks di tepi tiap fitur.

## Sumber kebenaran

Database (tabel `data_products`, kolom `file_path`) tetap menjadi sumber
kebenaran untuk path tiap file. Struktur folder di atas adalah konvensi yang
dipakai `etl/folder_manager.py` untuk *menghasilkan* path itu secara
konsisten — jangan mengasumsikan path dari nama file/folder saja di kode baru,
selalu pakai `etl/folder_manager.py` atau baca `data_products.file_path`.

Kolom `data_products.source` (`SENTINEL1` | `MODIS` | `GPM` | `FUSION`)
mencerminkan level `{source}` di path. Produk fusion dilabeli `FUSION`, bukan
`SENTINEL1`, walaupun barisnya menempel ke `scene_id` S1 — isinya gabungan tiga
sensor, jadi melabelinya SENTINEL1 akan membuat filter `?source=sentinel1` ikut
menarik file fusion.

`data/datasets/{id}_{slug}/metadata.json` adalah ringkasan read-only yang
ditulis ulang oleh orchestrator setiap kali sebuah job dataset selesai
(`COMPLETED`/`CANCELLED`/`PAUSED`). Isinya turunan dari tabel `datasets` plus
`storage_usage` per tier per source (dihitung `folder_manager.storage_breakdown`,
sumber angka yang sama dengan endpoint storage). Kalau berbeda dari API,
database yang benar.

Catatan: nama dataset dibekukan ke dalam nama folder saat file pertama kali
ditulis. Belum ada endpoint untuk mengganti nama dataset setelah dibuat; kalau
nanti ditambahkan, folder lama tidak otomatis berganti nama (perlu migrasi
serupa `etl/migrate_data_structure.py`) — `data_products.file_path` tetap jadi
sumber kebenaran meski folder & nama dataset saat itu tidak lagi cocok.

## `etl/folder_manager.py`

Satu-satunya tempat yang tahu cara menyusun path ini:

- `get_dataset_root(dataset_id, dataset_name)` → `data/datasets/{id}_{slug}`
- `get_tier_dir(dataset_id, dataset_name, tier)` → path satu tier
- `get_source_dir(dataset_id, dataset_name, tier, source)` → path satu source di satu tier
- `get_scene_dir(dataset_id, dataset_name, tier, source, scene_key)` → path satu scene
- `ensure_scene_dir(...)` → sama seperti di atas, sekaligus `mkdir`
- `get_fusion_dir(dataset_id, dataset_name, scene_key)` / `ensure_fusion_dir(...)` → folder fusion (tanpa level source)
- `get_scratch_dir(dataset_id, dataset_name, scene_key)` → folder `_work/{scene}` sementara
- `get_granule_cache_dir(dataset_id, dataset_name, source)` → cache granule mentah `raw/{source}`
- `list_sources(...)` / `list_scenes(...)` / `list_fusion_scenes(...)`
- `get_scene_files(...)` / `get_source_files(...)` / `get_tier_files(...)` / `get_fusion_scene_files(...)`
- `storage_breakdown(dataset_id, dataset_name)` → ringkasan ukuran per tier per source
- `write_dataset_metadata(...)` / `read_dataset_metadata(...)` / `get_dataset_metadata_path(...)`
- `slugify(name)` / `scene_slug(key)` / `date_key(d)` — sanitasi/normalisasi kunci
- `normalize_tier` / `normalize_source` / `sources_for_tier` / `db_source` — validasi

`etl/module5_orchestrator.py`, `etl/module4_gold_export.py`,
`etl/module7_modis_download.py`, `etl/module8_gpm_download.py`,
`etl/module9_fusion.py`, `etl/deletion_manager.py`, dan `api/routes/datasets.py`
semuanya memanggil modul ini alih-alih membangun `Path("data") / "datasets" / ...`
sendiri-sendiri, supaya kalau layout ini berubah lagi nanti, cukup satu file yang
diubah.

## Cleanup parsial per scene

Kalau `dataset.required_tiers` tidak termasuk tier tertentu (misalnya user
cuma minta `["FUSION"]`), orchestrator tetap memproses semua tier untuk sampai
ke fusion, lalu menghapus file tier yang tidak diminta **satu per satu**
berdasarkan path yang dicatat saat file itu dibuat (bukan `rmtree` seluruh
folder tier) — supaya file scene lain yang kebetulan diproses di tanggal yang
sama tidak ikut terhapus. Folder scene yang jadi kosong setelah itu dihapus
juga (`rmdir`, gagal-diam kalau ternyata belum kosong).

File MODIS/GPM yang ditulis saat menyiapkan input fusion ikut dilaporkan
`ensure_aux_inputs_for_date` ke orchestrator, jadi tier aux yang tidak diminta
juga ikut dibersihkan — tanpa itu `gold/modis` dan `gold/gpm` akan tertinggal
di disk saat user cuma meminta tier FUSION.

Produk Sentinel-1 ditandai `is_valid=False` lewat `scene_id`, produk aux
MODIS/GPM lewat `file_path`: baris aux menempel ke `SatelliteScene` placeholder
per-source-per-tanggal (`module9_fusion._resolve_aux_scene`), bukan ke scene S1
yang sedang diproses, jadi pencocokan lewat `scene_id` saja akan melewatkannya.

## Migrasi dari layout lama

`etl/migrate_data_structure.py` menerima dua layout asal sekaligus:

```
L1 (paling lama)  data/datasets/{dataset_id}/{YYYYMMDD}/{tier}/*
L2 (sebelumnya)   data/datasets/{dataset_id}_{slug}/{tier}/{scene}/*
```

```bash
psql -U postgres -d sentinel1_flood -f database/migrations/013_add_fusion_tier_and_source.sql
python -m etl.migrate_data_structure --dry-run          # lihat rencana dulu
python -m etl.migrate_data_structure                      # migrasi semua dataset
python -m etl.migrate_data_structure --dataset-id 2        # migrasi 1 dataset saja
```

Jalankan migrasi SQL 013 **sebelum** script ini: 013 yang menambah nilai enum
`FUSION` dan kolom `data_products.source`, dan yang memindahkan baris `FUSION_H5`
dari tier GOLD ke FUSION. Script Python-nya yang memindahkan file-nya. (File
`FUSION_H5` tetap mendarat di `fusion/` walaupun baris DB-nya masih tercatat
GOLD, jadi urutan terbalik tidak merusak data — cuma menyisakan baris DB yang
perlu diperbaiki manual.)

Kunci scene diambil dari `satellite_scenes.product_identifier` (via
`data_products.scene_id`) untuk produk S1, atau dari folder tanggal di layout
asal untuk artefak aux — dengan fallback ke tanggal di nama file kalau layout
asalnya tidak punya folder tanggal. File yang tidak tercatat di `data_products`
(arsip `.SAFE.zip` mentah, sidecar `metadata_qa.json`/`fusion_metadata.json`,
cache granule MODIS/GPM) ikut dipindah lewat pencocokan lokasi/isi file, bukan
lewat baris database.

Setiap dataset di-backup penuh ke `backup/data_structure_migration/dataset_{id}_{timestamp}/`
sebelum satu file pun dipindah. Setelah `shutil.move`, checksum SHA-256 file
di lokasi baru dibandingkan dengan `data_products.data_hash_sha256` — kalau
tidak cocok, file dikembalikan ke lokasi lama dan dilaporkan gagal (script
keluar dengan exit code 1 kalau ada yang gagal). `data_products.file_path` di
database di-update ke path baru untuk setiap file yang berhasil dipindah.
Script ini idempotent — file yang sudah ada di lokasi baru otomatis dilewati
kalau dijalankan lagi. Log lengkap ada di `logs_pipeline/migrate_data_structure.log`.

## API

- `GET /api/datasets/{dataset_id}/metadata` — isi `metadata.json` apa adanya.
- `GET /api/datasets/{dataset_id}/storage/summary` — ukuran, jumlah file, dan
  jumlah scene per tier, dipecah lagi per source, plus agregat per source lintas tier.
- `GET /api/datasets/{dataset_id}/storage/files/{tier}` — daftar file di satu
  tier, dikelompokkan per source dan scene. Terima `?source=` dan `?scene=`.
- `GET /api/datasets/{dataset_id}/download` — ZIP dataset. Terima `?tier=` dan
  `?source=` untuk mengunduh sebagian saja (mis. hanya GOLD MODIS tanpa ikut
  menarik puluhan GB tier RAW). Struktur folder di dalam ZIP selalu relatif ke
  root dataset, baik ZIP penuh maupun parsial.
- `GET /api/products?tier=GOLD&source=MODIS` — filter produk per tier **dan**
  source, lewat index `idx_dprods_tier_source`.
- `GET /api/quality/dataset/{dataset_id}/by-source` — kualitas per sensor.
  Perhatikan field `kind`: `SENTINEL1` melaporkan skor radiometrik sungguhan dari
  `quality_metrics`, sementara MODIS/GPM melaporkan *coverage* (berapa persen band
  yang diharapkan benar-benar ada). Keduanya bukan skala yang sama dan tidak boleh
  dibandingkan langsung — speckle index dan backscatter tidak berarti untuk curah
  hujan atau indeks vegetasi.

> `/api/storage/*` (di `api/routes/storage.py`) adalah endpoint lama dari
> arsitektur sebelum per-dataset folder ini ada — dia membaca `processed/{tier}/`
> global, bukan `data/datasets/{id}_{slug}/...`, dan tidak lagi merefleksikan di mana
> data sebenarnya tersimpan. Untuk statistik storage yang akurat, pakai
> endpoint `/api/datasets/{dataset_id}/storage/*` di atas.
