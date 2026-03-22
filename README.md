# AACR Spatial Keyword Scan

Static site and scraper for the AACR Annual Meeting 2026 keyword scan.

Published site:

- https://adytannu.github.io/aacr-spatial-keyword-scan/

Main artifacts:

- `docs/index.html`: keyword count summary
- `docs/combined_keyword_tables.html`: combined table view
- `docs/insights.html`: overlap, affiliation, and geography dashboard
- `scrape_aacr_keywords.py`: scraper and site generator

To regenerate locally:

```bash
python3 scrape_aacr_keywords.py --out-dir output
```
