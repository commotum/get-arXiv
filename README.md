# get-arXiv

Download arXiv author pages and cache API XML plus HTML abstracts.

**Usage**
1. `python main.py`
1. `python main.py <last-name> <first-name>`

Running with no arguments processes `authors.csv` in the project root. The file should have two columns: `last-name,first-name` (a header row is fine).

Running with a single author downloads that author immediately and appends them to `authors.csv` if they are not already listed.

Outputs are written under `AUTHORS/<last>-<first>/API` and `AUTHORS/<last>-<first>/HTML`.
