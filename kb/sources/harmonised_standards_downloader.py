"""Download harmonised-standards XLSX files from the EU Commission website.

Each NLF directive has a page at:
    https://single-market-economy.ec.europa.eu/single-market/goods/
        european-standards/harmonised-standards/<slug>_en

The page contains a download link to an XLSX (or occasionally XLS) file
with the "Summary list" of harmonised standards for that directive.

Two link patterns exist:
  A. /document/download/{uuid}_en?filename=...xlsx   (same domain)
  B. https://ec.europa.eu/docsroom/documents/{id}     (docsroom redirect)

This module scrapes each directive page, finds the spreadsheet link,
downloads it, and saves it to the output directory with a normalised
filename like ``2009_48_EC.xlsx``.

Usage:
    python -m kb.sources.harmonised_standards_downloader [--output-dir data/harmonised_standards]
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://single-market-economy.ec.europa.eu"
STANDARDS_PATH = "/single-market/goods/european-standards/harmonised-standards"

# Map directive_ref → page slug on the EU Commission site.
# Built from the index page at harmonised-standards_en.
DIRECTIVE_PAGES: dict[str, dict] = {
    "2006/42/EC": {
        "slug": "machinery-md",
        "short": "Machinery",
    },
    "2014/35/EU": {
        "slug": "low-voltage-lvd",
        "short": "LVD",
    },
    "2014/30/EU": {
        "slug": "electromagnetic-compatibility-emc",
        "short": "EMC",
    },
    "2017/745": {
        "slug": "medical-devices",
        "short": "MDR",
    },
    "2011/65/EU": {
        "slug": "restriction-use-certain-hazardous-substances-rohs",
        "short": "RoHS",
    },
    "2014/53/EU": {
        "slug": "radio-equipment",
        "short": "RED",
    },
    "2014/68/EU": {
        "slug": "pressure-equipment",
        "short": "PED",
    },
    "2009/48/EC": {
        "slug": "toy-safety",
        "short": "Toy Safety",
    },
    "2016/425": {
        "slug": "personal-protective-equipment",
        "short": "PPE",
    },
    "2016/426": {
        "slug": "gas-appliances",
        "short": "Gas Appliances",
    },
    "2014/34/EU": {
        "slug": "equipment-explosive-atmospheres-atex",
        "short": "ATEX",
    },
    "2014/33/EU": {
        "slug": "lifts",
        "short": "Lifts",
    },
    "2014/32/EU": {
        "slug": "measuring-instruments-mid",
        "short": "MID",
    },
    "2013/53/EU": {
        "slug": "recreational-craft",
        "short": "Recreational Craft",
    },
    "2014/29/EU": {
        "slug": "simple-pressure-vessels",
        "short": "SPVD",
    },
    "2014/90/EU": {
        "slug": "marine-equipment",
        "short": "Marine Equipment",
    },
    "305/2011": {
        "slug": "construction-products-cpdcpr",
        "short": "Construction Products",
    },
    "2014/31/EU": {
        "slug": "non-automatic-weighing-instruments-nawi",
        "short": "NAWI",
    },
    "2016/797": {
        "slug": "rail-system-interoperability",
        "short": "Railway",
    },
    "2017/746": {
        "slug": "iv-diagnostic-medical-devices",
        "short": "IVDR",
    },
    "2016/424": {
        "slug": "cableway-installations",
        "short": "Cableway",
    },
    "2013/29/EU": {
        "slug": "pyrotechnic-articles",
        "short": "Pyrotechnics",
    },
    "2014/28/EU": {
        "slug": "explosives-civil-uses",
        "short": "Explosives",
    },
    "2009/125/EC": {
        "slug": "ecodesign",
        "short": "Ecodesign",
    },
    # Legacy directives with their own pages
    "93/42/EEC": {
        "slug": "medical-devices-old",
        "short": "MDD (legacy)",
    },
    "90/385/EEC": {
        "slug": "implantable-medical-devices",
        "short": "AIMDD (legacy)",
    },
    "98/79/EC": {
        "slug": "iv-diagnostic-medical-devices-old",
        "short": "IVDD (legacy)",
    },
    # Additional directives seen on the index page
    "2000/14/EC": {
        "slug": "outdoor-noise",
        "short": "Outdoor Noise",
    },
}

# Directives that have NO harmonised-standards page on the Commission site.
# These are regulations/directives too new or too niche for an XLSX.
NO_PAGE_DIRECTIVES = {
    "2023/1230",     # New Machinery Regulation — not yet in force
    "2019/945",      # Drones (UAS) — separate page, may have standards
    "2010/35/EU",    # Transportable Pressure Equipment — no dedicated page
    "92/42/EEC",     # Boiler Efficiency (legacy) — no page
    "2010/30/EU",    # Energy Labelling — merged into ecodesign page
    "2014/94/EU",    # Alt Fuels Infrastructure — no harmonised standards page
}


def _normalise_filename(directive_ref: str) -> str:
    """'2006/42/EC' → '2006_42_EC.xlsx'."""
    return directive_ref.replace("/", "_") + ".xlsx"


def _find_xlsx_link(html: str) -> str | None:
    """Parse the HTML and find the XLSX/XLS download link."""
    soup = BeautifulSoup(html, "html.parser")

    # Pattern 1: direct link with .xlsx or .xls in href or filename param
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()

        # Match /document/download/ links with xlsx filename
        if "/document/download/" in href and (
            ".xlsx" in href.lower() or ".xls" in href.lower()
        ):
            return href

        # Match link text mentioning "xls" pointing to docsroom
        if "xls" in text and "docsroom" in href:
            return href

    # Pattern 2: links labelled "Summary list as xls" pointing anywhere
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        if "summary list" in text and "xls" in text:
            return a["href"]

    # Pattern 3: any docsroom link in context of "xls"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "docsroom" in href:
            # Check surrounding text
            parent_text = a.parent.get_text(strip=True).lower() if a.parent else ""
            if "xls" in parent_text or "spreadsheet" in parent_text:
                return href

    return None


def _resolve_url(href: str) -> str:
    """Turn a relative href into a full URL."""
    if href.startswith("http"):
        return href
    return BASE_URL + href


def download_xlsx(
    directive_ref: str,
    slug: str,
    output_dir: Path,
    client: httpx.Client,
) -> dict:
    """Download the XLSX for one directive. Returns a status dict."""
    filename = _normalise_filename(directive_ref)
    out_path = output_dir / filename
    page_url = f"{BASE_URL}{STANDARDS_PATH}/{slug}_en"

    result = {
        "directive_ref": directive_ref,
        "slug": slug,
        "filename": filename,
        "page_url": page_url,
        "status": "unknown",
        "error": "",
    }

    # Step 1: fetch the directive page
    try:
        resp = client.get(page_url, follow_redirects=True)
        if resp.status_code != 200:
            result["status"] = "page_error"
            result["error"] = f"HTTP {resp.status_code}"
            return result
    except httpx.HTTPError as exc:
        result["status"] = "page_error"
        result["error"] = str(exc)
        return result

    # Step 2: find the XLSX link
    xlsx_href = _find_xlsx_link(resp.text)
    if not xlsx_href:
        result["status"] = "no_xlsx_link"
        result["error"] = "No XLSX/XLS download link found on page"
        return result

    xlsx_url = _resolve_url(xlsx_href)
    result["xlsx_url"] = xlsx_url

    # Step 3: resolve docsroom URLs to direct download
    if "docsroom/documents" in xlsx_url:
        # Pattern: /docsroom/documents/{id} → .../attachments/1/translations/en/renditions/native
        m = re.search(r"docsroom/documents/(\d+)", xlsx_url)
        if m:
            doc_id = m.group(1)
            xlsx_url = (
                f"https://ec.europa.eu/docsroom/documents/{doc_id}"
                f"/attachments/1/translations/en/renditions/native"
            )

    # Step 4: download the file
    try:
        dl_resp = client.get(xlsx_url, follow_redirects=True)
        if dl_resp.status_code != 200:
            result["status"] = "download_error"
            result["error"] = f"HTTP {dl_resp.status_code} on XLSX download"
            return result
    except httpx.HTTPError as exc:
        result["status"] = "download_error"
        result["error"] = str(exc)
        return result

    # Step 5: validate it looks like an Excel file
    content = dl_resp.content
    if len(content) < 500:
        result["status"] = "download_error"
        result["error"] = f"File too small ({len(content)} bytes)"
        return result

    # Write to disk
    out_path.write_bytes(content)
    result["status"] = "ok"
    result["bytes"] = len(content)
    result["path"] = str(out_path)
    return result


def download_all(
    output_dir: Path,
    skip_existing: bool = True,
    delay: float = 2.0,
) -> list[dict]:
    """Download XLSX files for all known directives.

    Args:
        output_dir: directory to save XLSX files
        skip_existing: if True, skip directives that already have a file
        delay: seconds between requests (be polite to EU servers)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    client = httpx.Client(
        timeout=60,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (trade-compliance-kb/1.0; "
                "educational-research; +https://github.com)"
            ),
        },
    )

    directives = list(DIRECTIVE_PAGES.items())
    for i, (directive_ref, meta) in enumerate(directives):
        filename = _normalise_filename(directive_ref)
        existing = output_dir / filename

        if skip_existing and existing.exists():
            results.append({
                "directive_ref": directive_ref,
                "slug": meta["slug"],
                "filename": filename,
                "status": "skipped_existing",
            })
            logger.info(
                "[%d/%d] %s — skipped (exists)",
                i + 1, len(directives), directive_ref,
            )
            continue

        logger.info(
            "[%d/%d] %s (%s) — downloading...",
            i + 1, len(directives), directive_ref, meta["short"],
        )
        result = download_xlsx(directive_ref, meta["slug"], output_dir, client)
        results.append(result)

        if result["status"] == "ok":
            logger.info("  OK: %d bytes → %s", result["bytes"], result["filename"])
        else:
            logger.warning("  FAIL: %s — %s", result["status"], result["error"])

        if i < len(directives) - 1:
            time.sleep(delay)

    client.close()
    return results


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ap = argparse.ArgumentParser(
        description="Download harmonised-standards XLSX files from EU Commission",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/harmonised_standards"),
        help="Directory to save XLSX files (default: data/harmonised_standards)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay between requests in seconds (default: 2.0)",
    )
    args = ap.parse_args()

    results = download_all(
        output_dir=args.output_dir,
        skip_existing=not args.force,
        delay=args.delay,
    )

    # Summary
    ok = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skipped_existing"]
    failed = [r for r in results if r["status"] not in ("ok", "skipped_existing")]

    print(f"\nDownloaded: {len(ok)}")
    print(f"Skipped (existing): {len(skipped)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed directives:")
        for r in failed:
            print(f"  {r['directive_ref']:14s} {r['status']:20s} {r.get('error', '')}")

    if ok:
        print("\nNew downloads:")
        for r in ok:
            print(f"  {r['directive_ref']:14s} -> {r['filename']} ({r['bytes']} bytes)")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
