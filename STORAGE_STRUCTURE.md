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
        │   ├── {product_identifier}.SAFE/      # satu folder per scene Sentinel-1
        │   │   ├── {product_identifier}.SAFE.zip
        │   │   ├── ..._VV.tif, ..._VH.tif
        │   ├── _aux_modis/                      # cache granule NASA mentah (bukan scene)
        │   └── _aux_gpm/
        ├── bronze/
        │   └── {product_identifier}.SAFE/
        │       └── ..._VV_crop.tif, ..._VH_crop.tif
        ├── silver/
        │   ├── {product_identifier}.SAFE/       # scene S1
        │   │   ├── ..._VV_lee.tif, ..._VH_lee.tif
        │   │   └── metadata_qa.json              # metrik kualitas band untuk scene ini
        │   └── {YYYYMMDD}/                       # scene "aux" (bukan milik satu scene S1)
        │       ├── modis_{date}_flood.tif        # input fusion (module7), bukan deliverable
        │       └── gpm_rain_{window}_{date}.tif  # input fusion (module8)
        └── gold/
            └── {YYYYMMDD}/                        # scene = tanggal fusion (dedup per hari)
                ├── fusion_{date}.h5               # HDF5 multi-modal: S1 + MODIS + GPM
                └── fusion_metadata.json           # layers, bbox, timestamps, source scenes
```

- `{dataset_id}_{slug_nama_dataset}` — primary key `datasets.dataset_id` digabung
  slug dari `datasets.name` (mis. dataset bernama "hakim d1" -> folder `7_hakim_d1`).
  Slug: karakter selain `A-Za-z0-9_.-` diganti `_` (fungsi `folder_manager.slugify`,
  sama persis dengan `pipeline_logger.py:_slug_filename` yang dipakai untuk nama
  file log per-dataset).
- Setiap tier (`raw/bronze/silver/gold`) berisi satu subfolder per **scene**:
  - Untuk artefak Sentinel-1 (RAW_EXTRACTED_TIFF, CROPPED_TIFF, LEE_FILTERED),
    scene = `product_identifier` scene tsb (`satellite_scenes.product_identifier`,
    sudah unik termasuk jam:menit:detik).
  - Untuk artefak yang tidak terikat ke satu scene S1 tertentu — input fusion
    MODIS/GPM (SILVER) dan output fusion GOLD, yang di-dedup per tanggal — scene
    = tanggal akuisisi `YYYYMMDD`.
  - Folder berawalan `_` (mis. `_aux_modis`, `_aux_gpm` di bawah `raw/`) bukan
    scene: itu cache granule NASA mentah yang mencakup banyak tanggal sekaligus
    untuk satu dataset, aman karena `product_identifier` S1 asli tidak pernah
    diawali underscore.
- `_work/{scene}/` di level root dataset adalah folder kerja sementara (hasil
  kalibrasi radiometrik sebelum crop) — dihapus otomatis setelah tahap CROP
  selesai, bukan bagian dari 4 tier resmi.

## Tier

| Tier   | Dihasilkan oleh                                    | Isi                                                          |
|--------|-----------------------------------------------------|------------------------------------------------------------|
| raw    | `module1_download.py`                                | ZIP asli + TIFF hasil ekstrak per band                          |
| bronze | `module2_crop.py`                                    | TIFF setelah dipotong ke bbox AOI                                |
| silver | `module3_lee_filter.py`, `module7`/`module8`         | TIFF S1 setelah speckle filtering (Lee), + GeoTIFF harian MODIS flood / GPM rainfall (input fusion, bukan deliverable) |
| gold   | `module9_fusion.py`                                  | HDF5 multi-modal (S1 VV/VH + MODIS flood + GPM rainfall) + `fusion_metadata.json` — **satu-satunya** deliverable GOLD, tidak ada GeoTIFF per-band lagi |

`module6_analytics.py` menulis `metadata_qa.json` ke dalam folder scene S1 di
`silver/{product_identifier}.SAFE/` (band VV/VH sudah final di tier ini — GOLD
tidak lagi punya produk per-band), berisi metrik band VV/VH (quality score,
backscatter mean/std/min/max, speckle index, dsb). Metrik yang sama juga
disimpan di tabel `quality_metrics` — file JSON ini untuk siapa pun yang
menjelajah filesystem langsung tanpa akses database.

Pipeline penuh sekarang: DOWNLOAD (raw) → CROP (bronze) → LEE_FILTER (silver)
→ QUALITY_ANALYTICS (metrik atas silver) → FUSION (gold — mengambil input
MODIS/GPM lewat `module9_fusion.ensure_aux_inputs_for_date`, lalu memanggil
`module9_fusion.create_fusion_stack`). `module4_cog_export.py` sudah dihapus.

## Sumber kebenaran

Database (tabel `data_products`, kolom `file_path`) tetap menjadi sumber
kebenaran untuk path tiap file. Struktur folder di atas adalah konvensi yang
dipakai `etl/folder_manager.py` untuk *menghasilkan* path itu secara
konsisten — jangan mengasumsikan path dari nama file/folder saja di kode baru,
selalu pakai `etl/folder_manager.py` atau baca `data_products.file_path`.

`data/datasets/{id}_{slug}/metadata.json` adalah ringkasan read-only yang
ditulis ulang oleh orchestrator setiap kali sebuah job dataset selesai
(`COMPLETED`/`CANCELLED`/`PAUSED`). Isinya turunan dari tabel `datasets`, jadi
kalau berbeda dari API, database yang benar.

Catatan: nama dataset dibekukan ke dalam nama folder saat file pertama kali
ditulis. Belum ada endpoint untuk mengganti nama dataset setelah dibuat; kalau
nanti ditambahkan, folder lama tidak otomatis berganti nama (perlu migrasi
serupa `etl/migrate_data_structure.py`) — `data_products.file_path` tetap jadi
sumber kebenaran meski folder & nama dataset saat itu tidak lagi cocok.

## `etl/folder_manager.py`

Satu-satunya tempat yang tahu cara menyusun path ini:

- `get_dataset_root(dataset_id, dataset_name)` → `data/datasets/{id}_{slug}`
- `get_tier_dir(dataset_id, dataset_name, tier)` → path satu tier
- `get_scene_dir(dataset_id, dataset_name, tier, scene_key)` → path satu scene di satu tier
- `ensure_scene_dir(...)` → sama seperti di atas, sekaligus `mkdir`
- `get_scratch_dir(dataset_id, dataset_name, scene_key)` → folder `_work/{scene}` sementara
- `get_aux_raw_dir(dataset_id, dataset_name, source)` → cache granule mentah `raw/_aux_{source}`
- `list_scenes(dataset_id, dataset_name, tier)` → semua nama folder scene (bukan `_aux_*`) di satu tier
- `get_scene_files(...)` / `get_tier_files(...)` → list file di satu scene / satu tier
- `write_dataset_metadata(dataset_id, dataset_name, metadata)` / `get_dataset_metadata_path(...)`
- `slugify(name)` / `scene_slug(key)` — sanitasi nama dataset / kunci scene jadi aman-filesystem

`etl/module5_orchestrator.py`, `etl/module7_modis_download.py`,
`etl/module8_gpm_download.py`, `etl/module9_fusion.py`,
`etl/deletion_manager.py`, dan `api/routes/datasets.py` semuanya memanggil
modul ini alih-alih membangun `Path("data") / "datasets" / ...` sendiri-sendiri,
supaya kalau layout ini berubah lagi nanti, cukup satu file yang diubah.

## Cleanup parsial per scene

Kalau `dataset.required_tiers` tidak termasuk tier tertentu (misalnya user
cuma minta `["GOLD"]`), orchestrator tetap memproses semua tier untuk sampai
ke GOLD, lalu menghapus file tier yang tidak diminta **satu per satu**
berdasarkan path yang dicatat saat file itu dibuat (bukan `rmtree` seluruh
folder tier) — supaya file scene lain yang kebetulan diproses di tanggal yang
sama tidak ikut terhapus. Folder scene yang jadi kosong setelah itu dihapus
juga (`rmdir`, gagal-diam kalau ternyata belum kosong).

## Migrasi dari layout lama

Layout sebelumnya: `data/datasets/{dataset_id}/{acquisition_date_YYYYMMDD}/{tier}/*`
(tanggal-dulu, tanpa nama dataset di folder, tanpa subfolder per scene di
dalam tier). `etl/migrate_data_structure.py` memindahkan dataset yang masih
pakai layout itu ke layout tier-dulu-per-nama di atas:

```bash
python -m etl.migrate_data_structure --dry-run          # lihat rencana dulu
python -m etl.migrate_data_structure                      # migrasi semua dataset
python -m etl.migrate_data_structure --dataset-id 2        # migrasi 1 dataset saja
```

Kunci scene di layout baru diambil dari `satellite_scenes.product_identifier`
(via `data_products.scene_id`) untuk produk S1, atau dari folder tanggal lama
untuk artefak aux (`MODIS_FLOOD`/`GPM_RAINFALL`/`FUSION_H5`) — bukan diparse
ulang dari nama file. File yang tidak tercatat di `data_products` (arsip
`.SAFE.zip` mentah, sidecar `metadata_qa.json`/`fusion_metadata.json`, cache
granule MODIS/GPM) ikut dipindah lewat pencocokan lokasi/isi file, bukan lewat
baris database.

Setiap dataset di-backup penuh ke `backup/data_structure_migration/dataset_{id}_{timestamp}/`
sebelum satu file pun dipindah. Setelah `shutil.move`, checksum SHA-256 file
di lokasi baru dibandingkan dengan `data_products.data_hash_sha256` — kalau
tidak cocok, file dikembalikan ke lokasi lama dan dilaporkan gagal (script
keluar dengan exit code 1 kalau ada yang gagal). `data_products.file_path` di
database di-update ke path baru untuk setiap file yang berhasil dipindah.
Script ini idempotent — file yang sudah ada di lokasi baru otomatis dilewati
kalau dijalankan lagi. Log lengkap ada di `logs_pipeline/migrate_data_structure.log`.

## API

- `GET /api/datasets/{dataset_id}/storage/summary` — jumlah file, ukuran, dan
  jumlah scene per tier untuk dataset ini.
- `GET /api/datasets/{dataset_id}/storage/files/{tier}` — daftar file di satu
  tier, dikelompokkan per scene. Terima query `?scene=...` untuk filter satu
  scene (nama folder scene, mis. product_identifier atau tanggal YYYYMMDD).
- `GET /api/datasets/{dataset_id}/download` — ZIP seluruh dataset (semua
  tier & scene yang masih ada di disk).

> `/api/storage/*` (di `api/routes/storage.py`) adalah endpoint lama dari
> arsitektur sebelum per-dataset folder ini ada — dia membaca `processed/{tier}/`
> global, bukan `data/datasets/{id}_{slug}/...`, dan tidak lagi merefleksikan di mana
> data sebenarnya tersimpan. Untuk statistik storage yang akurat, pakai
> endpoint `/api/datasets/{dataset_id}/storage/*` di atas.
