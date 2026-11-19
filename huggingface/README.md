# GoldenCheetah datasets on Hugging Face

Tools to publish GoldenCheetah data on Hugging Face as Parquet, and the conversion of the
[GoldenCheetah OpenData](https://osf.io/6hfpz/) release itself.

- **[`CONVENTIONS.md`](CONVENTIONS.md)**: the rules every GoldenCheetah dataset on Hugging Face
  follows (column names, types and units, keys, layout, Parquet settings), so that code written
  for one dataset works on another. Defined once in [`conventions.py`](conventions.py).
- **[`datasets/opendata/`](datasets/opendata/)**: the migration of the OSF release to
  `GoldenCheetah/opendata`, lossless: every value in the original files can be recovered exactly.
- **[`datasets/_template/`](datasets/_template/)**: a small working dataset to start a new one from.
- **[`tools/`](tools/)**: `validate.py` checks a built dataset against its `dataset.toml` and the
  conventions; `smoke_test.py` reads an uploaded dataset back the way its users will.

Everything runs with [uv](https://docs.astral.sh/uv/): each script declares its dependencies inline
([PEP 723](https://peps.python.org/pep-0723/)), so `uv run <script>` is all it takes. Run commands
from this folder.

```
huggingface/
├── CONVENTIONS.md, conventions.py     the shared rules, and the code that applies them
├── datasets/
│   ├── opendata/                      the OpenData migration
│   │   ├── dataset.toml               what the dataset contains (tables, columns, keys, relations)
│   │   ├── README.md.tmpl             its dataset card
│   │   ├── gcod.py                    reading the OSF zips (pure functions)
│   │   └── migrate.py                 listing → mirror → convert → check → upload
│   └── _template/                     dataset.toml, card and build script to copy
├── tools/
│   ├── validate.py                    check a built dataset (layout, schema, values, relations, card)
│   └── smoke_test.py                  read a dataset back from the Hub with polars, DuckDB, datasets
├── tests/                             tests of all of the above, on made-up data
└── research/                          the exploration the design is based on (frozen)
```

## Migrating OpenData

```bash
uv run datasets/opendata/migrate.py listing     # OSF file listing with sha256
uv run datasets/opendata/migrate.py mirror      # all 6,613 zips, 115.6 GB, sha256-verified (~10-20 h)
uv run datasets/opendata/migrate.py convert     # -> data/build/opendata/ (~2 h)
uv run datasets/opendata/migrate.py check       # validate, and compare every value with the source (~1 h)
uv run datasets/opendata/migrate.py upload --repo GoldenCheetah/opendata
uv run tools/smoke_test.py datasets/opendata/dataset.toml GoldenCheetah/opendata --local data/build/opendata
```

Every stage resumes where it stopped and is safe to re-run. `upload` needs `hf auth login` with
write access, and uploads only a build that `check` passed unchanged. `uv run
datasets/opendata/migrate.py --help` has the details, including running the mirror unattended. Work
files go to `data/` (git-ignored). Conversion is deterministic: the same zips and converter
version give byte-identical files.

## Adding a dataset

See [Adding a dataset](CONVENTIONS.md#adding-a-dataset): copy the template, declare the tables in
`dataset.toml`, build, and validate against OpenData with `tools/validate.py --against`.

## Development

```bash
uv run --with-requirements tests/requirements.txt pytest tests -q
uvx ruff check
uv run conventions.py --check-docs      # after changing conventions.py: --write-docs
```

CI runs these on every pull request that touches this folder. `datasets/opendata/migrate.py.lock`
pins the exact libraries the dataset is built with, so a rebuild gives the same bytes; update it
with `uv lock --script datasets/opendata/migrate.py --upgrade`. When a change alters the
converter's output for the same input, bump `CONVERTER_VERSION` in `datasets/opendata/gcod.py`;
`convert` then rebuilds what changed. Changes to the conventions bump `conventions.VERSION`
(see [Versioning](CONVENTIONS.md#versioning)).
