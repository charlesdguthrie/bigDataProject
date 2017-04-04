# bigDataProject
Repository for our DS1004 Big Data Project

# Getting started
`faq` assumes you don't know anything about using it, so we baked in some hand-holding to get you started. Start by running the following from the terminal:

```bash
$ spark-submit faq.py
```

Assuming you have all required dependencies (Spark), this should output a helpful prompt. This prompt will tell you how to proceed, but basically reduces to the following:

1. With only an input CSV, `faq` will give you a pretty-print of the column names:

  ```bash
  $ spark-submit faq.py <input-file>.csv
  ```

2. With both an input CSV *and* space-separated strings, `faq` will perform the actual analysis on the valid columns:

  ```bash
  $ spark-submit faq.py <input-file>.csv 'column 1' 'column 2'
  ```


