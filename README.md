# get-arXiv

Download arXiv author pages and cache API XML plus HTML abstracts.

**Usage**
1. `python main.py`
1. `python main.py --sync-remote`
1. `python main.py <last-name> <first-name>`

Running with no arguments processes `authors.csv` in the project root. The file should have two columns: `last-name,first-name` (a header row is fine).

By default, batch mode (`python main.py`) uses cached API pages first, which is much faster when most authors are already downloaded.

Use `python main.py --sync-remote` when you want to force a fresh arXiv API check for every author (to catch newly submitted papers).

Running with a single author downloads that author immediately and appends them to `authors.csv` if they are not already listed.

Outputs are written under `AUTHORS/<last>-<first>/API` and `AUTHORS/<last>-<first>/HTML`.
