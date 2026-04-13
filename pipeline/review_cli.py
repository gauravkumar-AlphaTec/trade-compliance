"""Review CLI for pipeline validation_queue and KB verification_queue.

Usage:
    python -m pipeline.review_cli pipeline list
    python -m pipeline.review_cli pipeline show <id>
    python -m pipeline.review_cli pipeline resolve <id> [--note "reason"]
    python -m pipeline.review_cli pipeline dismiss <id>

    python -m pipeline.review_cli kb list [--country DE] [--issue low_confidence]
    python -m pipeline.review_cli kb show <id>
    python -m pipeline.review_cli kb accept <id>
    python -m pipeline.review_cli kb reject <id> [--note "reason"]
    python -m pipeline.review_cli kb edit <id>
    python -m pipeline.review_cli kb stats [--country DE]
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text

console = Console()

# KB tables with confidence/last_verified_at fields for stats
KB_TRACKED_TABLES = {
    "kb_memberships": ("country_id",),
    "kb_mras": ("country_id",),
    "kb_laws": ("country_id",),
    "kb_regulatory_authorities": ("country_id",),
    "kb_standards_acceptance": ("country_id",),
    "kb_testing_protocols": ("country_id",),
    "kb_national_deviations": ("country_id",),
}

# JSONB fields on kb_country_profiles that carry embedded provenance
KB_PROFILE_JSONB_FIELDS = (
    "national_standards_body",
    "accreditation_body",
    "metrology_institute",
    "legal_metrology_body",
)

# Allowlist of updatable plain columns on kb_country_profiles
PROFILE_UPDATABLE_FIELDS = frozenset({
    "country_name", "region", "ca_system_structure",
    "accreditation_mandatory", "translation_requirements",
    "translation_notes", "local_challenges", "recent_reforms",
    "general_notes", "notified_bodies_url",
})


def get_connection():
    """Open a DB connection using DATABASE_URL."""
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor,
    )


# ------------------------------------------------------------------
# Pipeline subcommands
# ------------------------------------------------------------------

def pipeline_list(conn):
    """List pending items in validation_queue."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, record_type, issue_type, issue_detail, created_at
            FROM validation_queue
            WHERE status = 'pending'
            ORDER BY created_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        console.print("[yellow]No pending pipeline items.[/yellow]")
        return

    table = Table(title="Pipeline Validation Queue")
    table.add_column("ID", style="cyan", justify="right")
    table.add_column("Record Type", style="green")
    table.add_column("Issue Type", style="red")
    table.add_column("Issue Detail", max_width=60)
    table.add_column("Created At")

    for row in rows:
        detail = row["issue_detail"] or ""
        if len(detail) > 60:
            detail = detail[:57] + "..."
        table.add_row(
            str(row["id"]),
            row["record_type"] or "",
            row["issue_type"] or "",
            detail,
            str(row["created_at"] or ""),
        )

    console.print(table)
    console.print(f"\n[dim]{len(rows)} pending item(s)[/dim]")


def pipeline_show(conn, item_id: int):
    """Show full detail for a pipeline queue item."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT vq.*,
                   s.url AS source_url
            FROM validation_queue vq
            LEFT JOIN regulations r ON vq.record_id = r.id
                AND vq.record_type = 'regulation'
            LEFT JOIN sources s ON r.source_id = s.id
            WHERE vq.id = %s
            """,
            (item_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if not row:
        console.print(f"[red]Item {item_id} not found.[/red]")
        return

    panel_lines = []
    for key in ("id", "record_type", "record_id", "issue_type",
                "issue_detail", "status", "created_at"):
        panel_lines.append(f"[bold]{key}:[/bold] {row.get(key, '')}")

    source_url = row.get("source_url") or "N/A"
    panel_lines.append(f"[bold]source_url:[/bold] {source_url}")

    console.print(Panel("\n".join(panel_lines), title=f"Pipeline Item #{item_id}"))


def pipeline_resolve(conn, item_id: int, note: str | None = None):
    """Mark a pipeline queue item as resolved."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE validation_queue
            SET status = 'resolved'
            WHERE id = %s AND status = 'pending'
            RETURNING id
            """,
            (item_id,),
        )
        updated = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    if updated:
        console.print(f"[green]Item {item_id} resolved.[/green]")
    else:
        console.print(f"[red]Item {item_id} not found or already processed.[/red]")


def pipeline_dismiss(conn, item_id: int):
    """Mark a pipeline queue item as dismissed."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE validation_queue
            SET status = 'dismissed'
            WHERE id = %s AND status = 'pending'
            RETURNING id
            """,
            (item_id,),
        )
        updated = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    if updated:
        console.print(f"[yellow]Item {item_id} dismissed.[/yellow]")
    else:
        console.print(f"[red]Item {item_id} not found or already processed.[/red]")


# ------------------------------------------------------------------
# KB subcommands
# ------------------------------------------------------------------

def kb_list(conn, country: str | None = None, issue: str | None = None):
    """List pending items in kb_verification_queue."""
    conditions = ["vq.status = 'pending'"]
    params: list = []

    if country:
        conditions.append("cp.iso2 = %s")
        params.append(country)
    if issue and issue != "all":
        conditions.append("vq.issue_type = %s")
        params.append(issue)

    where = " AND ".join(conditions)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT vq.id, cp.iso2 AS country, vq.table_name,
                   vq.field_name, vq.issue_type, vq.confidence,
                   vq.created_at
            FROM kb_verification_queue vq
            LEFT JOIN kb_country_profiles cp ON vq.country_id = cp.id
            WHERE {where}
            ORDER BY vq.created_at DESC
            """,
            params,
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        console.print("[yellow]No pending KB items.[/yellow]")
        return

    table = Table(title="KB Verification Queue")
    table.add_column("ID", style="cyan", justify="right")
    table.add_column("Country", style="green")
    table.add_column("Table")
    table.add_column("Field")
    table.add_column("Issue Type", style="red")
    table.add_column("Confidence", justify="right")
    table.add_column("Created At")

    for row in rows:
        conf = f"{row['confidence']:.2f}" if row["confidence"] is not None else ""
        table.add_row(
            str(row["id"]),
            row["country"] or "",
            row["table_name"] or "",
            row["field_name"] or "",
            row["issue_type"] or "",
            conf,
            str(row["created_at"] or ""),
        )

    console.print(table)
    console.print(f"\n[dim]{len(rows)} pending item(s)[/dim]")


def kb_show(conn, item_id: int):
    """Show side-by-side current vs proposed value for a KB queue item."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT vq.*, cp.iso2 AS country
            FROM kb_verification_queue vq
            LEFT JOIN kb_country_profiles cp ON vq.country_id = cp.id
            WHERE vq.id = %s
            """,
            (item_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if not row:
        console.print(f"[red]Item {item_id} not found.[/red]")
        return

    current_panel = Panel(
        row.get("current_value") or "[dim]empty[/dim]",
        title="Current Value",
        border_style="red",
    )
    proposed_panel = Panel(
        row.get("proposed_value") or "[dim]empty[/dim]",
        title="Proposed Value",
        border_style="green",
    )

    console.print(Panel(
        f"[bold]Country:[/bold] {row.get('country', 'N/A')}  "
        f"[bold]Table:[/bold] {row.get('table_name', '')}  "
        f"[bold]Field:[/bold] {row.get('field_name', '')}  "
        f"[bold]Issue:[/bold] {row.get('issue_type', '')}  "
        f"[bold]Confidence:[/bold] {row.get('confidence', '')}",
        title=f"KB Item #{item_id}",
    ))

    console.print(Columns([current_panel, proposed_panel], equal=True))

    source_url = row.get("source_url") or "N/A"
    conflict_url = row.get("conflict_source_url") or "N/A"
    console.print(f"[bold]Source URL:[/bold] {source_url}")
    console.print(f"[bold]Conflict URL:[/bold] {conflict_url}")


def kb_accept(conn, item_id: int):
    """Accept proposed value: write to target table, update confidence, resolve item."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM kb_verification_queue WHERE id = %s AND status = 'pending'",
            (item_id,),
        )
        item = cur.fetchone()
        if not item:
            console.print(f"[red]Item {item_id} not found or already processed.[/red]")
            return

        table_name = item["table_name"]
        record_id = item["record_id"]
        field_name = item["field_name"]
        proposed = item["proposed_value"]

        if not table_name or not field_name:
            console.print("[red]Cannot accept: missing table_name or field_name.[/red]")
            return

        # Write proposed_value to the target table
        if table_name == "kb_country_profiles" and field_name in KB_PROFILE_JSONB_FIELDS:
            _update_jsonb_field(cur, record_id, field_name, proposed)
        elif table_name == "kb_country_profiles":
            _update_profile_field(cur, record_id, field_name, proposed)
        else:
            _update_related_field(cur, table_name, record_id, field_name, proposed)

        # Mark queue item resolved
        cur.execute(
            """
            UPDATE kb_verification_queue
            SET status = 'resolved', resolved_at = NOW()
            WHERE id = %s
            """,
            (item_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    console.print(f"[green]Item {item_id} accepted. Value written to {table_name}.{field_name}.[/green]")


def _update_jsonb_field(cur, record_id: int, field_name: str, proposed: str):
    """Update a JSONB provenance field on kb_country_profiles."""
    cur.execute(
        f"""
        UPDATE kb_country_profiles
        SET {field_name} = jsonb_set(
                COALESCE({field_name}, '{{}}'::jsonb),
                '{{value}}',
                to_jsonb(%s::text)
            ),
            updated_at = NOW()
        WHERE id = %s
        """,
        (proposed, record_id),
    )


def _update_profile_field(cur, record_id: int, field_name: str, proposed: str):
    """Update a plain field on kb_country_profiles."""
    if field_name not in PROFILE_UPDATABLE_FIELDS:
        raise ValueError(f"Field {field_name} is not updatable via review CLI")

    cur.execute(
        f"""
        UPDATE kb_country_profiles
        SET {field_name} = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (proposed, record_id),
    )


def _update_related_field(cur, table_name: str, record_id: int,
                          field_name: str, proposed: str):
    """Update a field on a related KB table, set confidence=0.95, last_verified_at=NOW()."""
    if table_name not in KB_TRACKED_TABLES:
        raise ValueError(f"Table {table_name} is not updatable via review CLI")

    cur.execute(
        f"""
        UPDATE {table_name}
        SET {field_name} = %s,
            confidence = 0.95,
            last_verified_at = NOW()
        WHERE id = %s
        """,
        (proposed, record_id),
    )


def kb_reject(conn, item_id: int, note: str | None = None):
    """Reject proposed value: keep current, mark dismissed."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE kb_verification_queue
            SET status = 'dismissed',
                resolved_at = NOW(),
                reviewer_note = %s
            WHERE id = %s AND status = 'pending'
            RETURNING id
            """,
            (note, item_id),
        )
        updated = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    if updated:
        console.print(f"[yellow]Item {item_id} rejected.[/yellow]")
    else:
        console.print(f"[red]Item {item_id} not found or already processed.[/red]")


def kb_edit(conn, item_id: int):
    """Prompt for manual entry and write with confidence=1.0."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM kb_verification_queue WHERE id = %s AND status = 'pending'",
            (item_id,),
        )
        item = cur.fetchone()
        if not item:
            console.print(f"[red]Item {item_id} not found or already processed.[/red]")
            return

        table_name = item["table_name"]
        record_id = item["record_id"]
        field_name = item["field_name"]

        if not table_name or not field_name:
            console.print("[red]Cannot edit: missing table_name or field_name.[/red]")
            return

        console.print(f"[bold]Table:[/bold] {table_name}")
        console.print(f"[bold]Field:[/bold] {field_name}")
        console.print(f"[bold]Current:[/bold] {item.get('current_value', 'N/A')}")
        console.print(f"[bold]Proposed:[/bold] {item.get('proposed_value', 'N/A')}")

        new_value = console.input("\n[bold cyan]Enter new value:[/bold cyan] ")
        if not new_value.strip():
            console.print("[yellow]Cancelled — empty value.[/yellow]")
            return

        # Write the manual value
        if table_name == "kb_country_profiles" and field_name in KB_PROFILE_JSONB_FIELDS:
            _update_jsonb_field(cur, record_id, field_name, new_value)
        elif table_name == "kb_country_profiles":
            _update_profile_field(cur, record_id, field_name, new_value)
        else:
            if table_name not in KB_TRACKED_TABLES:
                console.print(f"[red]Table {table_name} is not updatable.[/red]")
                return
            cur.execute(
                f"""
                UPDATE {table_name}
                SET {field_name} = %s,
                    confidence = 1.0,
                    last_verified_at = NOW()
                WHERE id = %s
                """,
                (new_value, record_id),
            )

        # Resolve the queue item
        cur.execute(
            """
            UPDATE kb_verification_queue
            SET status = 'resolved',
                resolved_at = NOW(),
                reviewer_note = %s
            WHERE id = %s
            """,
            (f"manual edit: {new_value}", item_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    console.print(f"[green]Item {item_id} updated with manual value (confidence=1.0).[/green]")


def kb_stats(conn, country: str | None = None):
    """Country completeness report."""
    cur = conn.cursor()
    try:
        if country:
            cur.execute(
                "SELECT id, iso2, country_name FROM kb_country_profiles WHERE iso2 = %s",
                (country,),
            )
            countries = cur.fetchall()
            if not countries:
                console.print(f"[red]Country {country} not found.[/red]")
                return
        else:
            cur.execute(
                "SELECT id, iso2, country_name FROM kb_country_profiles ORDER BY iso2"
            )
            countries = cur.fetchall()

        if not countries:
            console.print("[yellow]No countries in KB.[/yellow]")
            return

        table = Table(title="KB Completeness Report")
        table.add_column("Country", style="cyan")
        table.add_column("Total Fields", justify="right")
        table.add_column("Populated", justify="right", style="green")
        table.add_column("Missing", justify="right", style="red")
        table.add_column("Pending Review", justify="right", style="yellow")
        table.add_column("Stale (>90d)", justify="right", style="magenta")

        for c in countries:
            cid = c["id"]
            total, populated, stale = _count_fields(cur, cid)
            missing = total - populated

            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM kb_verification_queue
                WHERE country_id = %s AND status = 'pending'
                """,
                (cid,),
            )
            pending = cur.fetchone()["cnt"]

            table.add_row(
                f"{c['iso2']} ({c['country_name']})",
                str(total),
                str(populated),
                str(missing),
                str(pending),
                str(stale),
            )

        console.print(table)
    finally:
        cur.close()


def _count_fields(cur, country_id: int) -> tuple[int, int, int]:
    """Count total, populated, and stale fields for a country.

    Returns (total, populated, stale).
    """
    total = 0
    populated = 0
    stale = 0

    # Count related table rows (each row = 1 populated entry)
    for tbl, (fk_col,) in KB_TRACKED_TABLES.items():
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE {fk_col} = %s",
            (country_id,),
        )
        row_count = cur.fetchone()["cnt"]
        total += row_count
        populated += row_count

        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM {tbl}
            WHERE {fk_col} = %s
              AND (last_verified_at IS NULL
                   OR last_verified_at < NOW() - INTERVAL '90 days')
            """,
            (country_id,),
        )
        stale += cur.fetchone()["cnt"]

    # Count JSONB provenance fields on kb_country_profiles
    for field in KB_PROFILE_JSONB_FIELDS:
        total += 1
        cur.execute(
            f"SELECT {field} FROM kb_country_profiles WHERE id = %s",
            (country_id,),
        )
        row = cur.fetchone()
        if row and row[field] is not None:
            populated += 1
            val = row[field]
            if isinstance(val, dict):
                lv = val.get("last_verified_at")
                if not lv:
                    stale += 1
            else:
                stale += 1

    return total, populated, stale


# ------------------------------------------------------------------
# Argument parser
# ------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="review_cli",
        description="Review CLI for pipeline and KB queues",
    )
    subparsers = parser.add_subparsers(dest="group", required=True)

    # --- pipeline ---
    pipeline_parser = subparsers.add_parser("pipeline", help="Pipeline validation queue")
    pipeline_sub = pipeline_parser.add_subparsers(dest="command", required=True)

    pipeline_sub.add_parser("list", help="List pending items")

    show_p = pipeline_sub.add_parser("show", help="Show item detail")
    show_p.add_argument("id", type=int)

    resolve_p = pipeline_sub.add_parser("resolve", help="Resolve an item")
    resolve_p.add_argument("id", type=int)
    resolve_p.add_argument("--note", type=str, default=None)

    dismiss_p = pipeline_sub.add_parser("dismiss", help="Dismiss an item")
    dismiss_p.add_argument("id", type=int)

    # --- kb ---
    kb_parser = subparsers.add_parser("kb", help="KB verification queue")
    kb_sub = kb_parser.add_subparsers(dest="command", required=True)

    list_kb = kb_sub.add_parser("list", help="List pending KB items")
    list_kb.add_argument("--country", type=str, default=None)
    list_kb.add_argument("--issue", type=str, default=None,
                         help="Filter: low_confidence|spot_check|all")

    show_kb = kb_sub.add_parser("show", help="Show KB item detail")
    show_kb.add_argument("id", type=int)

    accept_kb = kb_sub.add_parser("accept", help="Accept proposed value")
    accept_kb.add_argument("id", type=int)

    reject_kb = kb_sub.add_parser("reject", help="Reject proposed value")
    reject_kb.add_argument("id", type=int)
    reject_kb.add_argument("--note", type=str, default=None)

    edit_kb = kb_sub.add_parser("edit", help="Manual edit")
    edit_kb.add_argument("id", type=int)

    stats_kb = kb_sub.add_parser("stats", help="Country completeness report")
    stats_kb.add_argument("--country", type=str, default=None)

    return parser


def main(argv: list[str] | None = None):
    parser = build_parser()
    args = parser.parse_args(argv)

    conn = get_connection()
    try:
        if args.group == "pipeline":
            if args.command == "list":
                pipeline_list(conn)
            elif args.command == "show":
                pipeline_show(conn, args.id)
            elif args.command == "resolve":
                pipeline_resolve(conn, args.id, getattr(args, "note", None))
            elif args.command == "dismiss":
                pipeline_dismiss(conn, args.id)

        elif args.group == "kb":
            if args.command == "list":
                kb_list(conn, args.country, args.issue)
            elif args.command == "show":
                kb_show(conn, args.id)
            elif args.command == "accept":
                kb_accept(conn, args.id)
            elif args.command == "reject":
                kb_reject(conn, args.id, getattr(args, "note", None))
            elif args.command == "edit":
                kb_edit(conn, args.id)
            elif args.command == "stats":
                kb_stats(conn, args.country)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
