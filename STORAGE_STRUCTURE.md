# Storage Structure

Bagaimana `data/datasets/` ditata di disk, dan bagaimana ETL, database, dan API
saling sinkron soal path.

## Layout

```
data/
└── datasets/
    └── {dataset_id}/
        ├── metadata.json                  # ringkasan level-dataset (lihat di bawah)
        ├── {acquisition_date_YYYYMMDD}/
        │   ├── raw/
        │   │   ├── S1A_..._VV.tif
        │   │   └── S1A_..._VH.tif
        │   ├── bronze/
        │   │   ├── S1A_..._VV_crop.tif
        │   │   └── S1A_..._VH_crop.tif
        │   ├── silver/
        │   │   ├── S1A_..._VV_lee.tif
        │   │   └── S1A_..._VH_lee.tif
        │   └── gold/
        │       ├── S1A_..._VV_cog.tif
        │       ├── S1A_..._VH_cog.tif
        │       └── metadata_qa.json       # metrik kualitas band untuk scene ini
        └── {acquisition_date_YYYYMMDD}/
            └── ...
```

- `{dataset_id}` — primary key `datasets.dataset_id` di Postgres.
- `{acquisition_date_YYYYMMDD}` — tanggal akuisisi scene (`satellite_scenes.acquisition_datetime`,
  bukan tanggal saat pipeline dijalankan).
- File di dalam satu tier **tidak** punya sub-folder per scene — nama file sudah
  memuat `product_identifier` Sentinel-1 lengkap (termasuk jam:menit:detik), jadi
  otomatis unik walau ada beberapa scene di tanggal yang sama.
- `.calib_work/` adalah folder kerja sementara (hasil kalibrasi radiometrik
  sebelum crop) — dihapus otomatis setelah tahap CROP selesai, bukan bagian dari
  4 tier resmi.

## Tier

| Tier   | Dihasilkan oleh          | Isi                                      |
|--------|---------------------------|-------------------------------------------|
| raw    | `module1_download.py`     | ZIP asli + TIFF hasil ekstrak per band     |
| bronze | `module2_crop.py`         | TIFF setelah dipotong ke bbox AOI          |
| silver | `module3_lee_filter.py`   | TIFF setelah speckle filtering (Lee)       |
| gold   | `module4_cog_export.py`   | Cloud-Optimized GeoTIFF, siap dipakai       |

`module6_analytics.py` menulis `metadata_qa.json` ke dalam folder `gold/`
scene yang bersangkutan, berisi metrik band VV/VH (quality score, backscatter
mean/std/min/max, speckle index, dsb). Metrik yang sama juga disimpan di tabel
`quality_metrics` — file JSON ini untuk siapa pun yang menjelajah filesystem
langsung tanpa akses database.

## Sumber kebenaran

Database (tabel `data_products`, kolom `file_path`) tetap menjadi sumber
kebenaran untuk path tiap file. Struktur folder di atas adalah konvensi yang
dipakai `etl/folder_manager.py` untuk *menghasilkan* path itu secara
konsisten — jangan mengasumsikan path dari nama file/folder saja di kode baru,
selalu pakai `etl/folder_manager.py` atau baca `data_products.file_path`.

`data/datasets/{dataset_id}/metadata.json` adalah ringkasan read-only yang
ditulis ulang oleh orchestrator setiap kali sebuah job dataset selesai
(`COMPLETED`/`CANCELLED`/`PAUSED`). Isinya turunan dari tabel `datasets`, jadi
kalau berbeda dari API, database yang benar.

## `etl/folder_manager.py`

Satu-satunya tempat yang tahu cara menyusun path ini:

- `get_dataset_root(dataset_id)` → `data/datasets/{id}`
- `get_dataset_path(dataset_id, acquisition_date, tier=None)` → path tanggal (+ tier opsional)
- `ensure_tier_folders_exist(dataset_id, acquisition_date)` → buat raw/bronze/silver/gold sekaligus
- `get_tier_files(dataset_id, acquisition_date, tier)` → list file di satu tier
- `list_acquisition_dates(dataset_id)` → semua folder tanggal yang ada untuk dataset ini
- `write_dataset_metadata(dataset_id, metadata)` / `get_dataset_metadata_path(dataset_id)`

`etl/module5_orchestrator.py`, `etl/deletion_manager.py`, dan
`api/routes/datasets.py` semuanya memanggil modul ini alih-alih membangun
`Path("data") / "datasets" / ...` sendiri-sendiri, supaya kalau layout ini
berubah lagi nanti, cukup satu file yang diubah.

## Cleanup parsial per scene

Kalau `dataset.required_tiers` tidak termasuk tier tertentu (misalnya user
cuma minta `["GOLD"]`), orchestrator tetap memproses semua tier untuk sampai
ke GOLD, lalu menghapus file tier yang tidak diminta **satu per satu**
berdasarkan path yang dicatat saat file itu dibuat (bukan `rmtree` seluruh
folder tier) — supaya file scene lain yang kebetulan diproses di tanggal yang
sama tidak ikut terhapus. Folder tier yang jadi kosong setelah itu dihapus
juga (`rmdir`, gagal-diam kalau ternyata belum kosong).

## Migrasi dari layout lama

Layout lama: `data/datasets/{dataset_id}/{tier}/{slug}/*.tif` (tanpa folder
tanggal, `{tier}` huruf kecil langsung di bawah dataset, `{slug}` = product
identifier yang disanitasi).

`etl/migrate_data_structure.py` memindahkan dataset yang masih pakai layout
lama ke layout baru, dengan tanggal akuisisi diambil dari
`satellite_scenes.acquisition_datetime` di database (bukan diparse dari nama
folder lama):

```bash
python -m etl.migrate_data_structure --dry-run          # lihat rencana dulu
python -m etl.migrate_data_structure                      # migrasi semua dataset
python -m etl.migrate_data_structure --dataset-id 2        # migrasi 1 dataset saja
```

Setiap dataset di-backup penuh ke `backup/data_structure_migration/dataset_{id}_{timestamp}/`
sebelum satu file pun dipindah. Setelah `shutil.move`, checksum SHA-256 file
di lokasi baru dibandingkan dengan `data_products.data_hash_sha256` — kalau
tidak cocok, file dikembalikan ke lokasi lama dan dilaporkan gagal (script
keluar dengan exit code 1 kalau ada yang gagal). `data_products.file_path` di
database di-update ke path baru untuk setiap file yang berhasil dipindah. Log
lengkap ada di `logs_pipeline/migrate_data_structure.log`.

## API

- `GET /api/datasets/{dataset_id}/storage/summary` — jumlah file & ukuran per
  tier untuk dataset ini (menjumlahkan semua tanggal akuisisi).
- `GET /api/datasets/{dataset_id}/storage/files/{tier}` — daftar file di satu
  tier, dikelompokkan per tanggal akuisisi. Terima query `?acquisition_date=YYYYMMDD`
  untuk filter satu tanggal.
- `GET /api/datasets/{dataset_id}/download` — ZIP seluruh dataset (semua
  tanggal, semua tier yang masih ada di disk).

> `/api/storage/*` (di `api/routes/storage.py`) adalah endpoint lama dari
> arsitektur sebelum per-dataset folder ini ada — dia membaca `processed/{tier}/`
> global, bukan `data/datasets/{id}/...`, dan tidak lagi merefleksikan di mana
> data sebenarnya tersimpan. Untuk statistik storage yang akurat, pakai
> endpoint `/api/datasets/{dataset_id}/storage/*` di atas.
