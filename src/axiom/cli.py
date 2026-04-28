"""Command-line interface for the law archive."""

from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from axiom.archive import AxiomArchive
from axiom.fetchers.irs_bulk import IRSBulkFetcher
from axiom.models_guidance import GuidanceType
from axiom.parsers.us.statutes import download_title
from axiom.storage.guidance import GuidanceStorage

console = Console()


@click.group()
@click.option("--db", default="axiom.db", help="Path to database file")
@click.pass_context
def main(ctx: click.Context, db: str):
    """Axiom Law Archive - Open source US statute text via API."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db


@main.command()
@click.argument("citation")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def get(ctx: click.Context, citation: str, as_json: bool):
    """Get a section by citation.

    Examples:
        axiom-corpus get "26 USC 32"
        axiom-corpus get "26 USC 32(a)(1)"
    """
    archive = AxiomArchive(db_path=ctx.obj["db"])
    section = archive.get(citation)

    if not section:
        console.print(f"[red]Not found:[/red] {citation}")
        raise SystemExit(1)

    if as_json:
        console.print_json(section.model_dump_json())
    else:
        console.print(
            Panel(
                f"[bold]{section.citation.usc_cite}[/bold]\n"
                f"[dim]{section.title_name}[/dim]\n\n"
                f"[bold blue]{section.section_title}[/bold blue]\n\n"
                f"{section.text[:2000]}{'...' if len(section.text) > 2000 else ''}\n\n"
                f"[dim]Source: {section.source_url}[/dim]",
                title=section.citation.usc_cite,
            )
        )


@main.command()
@click.argument("query")
@click.option("--title", "-t", type=int, help="Limit to specific title")
@click.option("--limit", "-n", default=10, help="Maximum results")
@click.pass_context
def search(ctx: click.Context, query: str, title: int | None, limit: int):
    """Search for sections matching a query.

    Examples:
        axiom-corpus search "earned income"
        axiom-corpus search "child tax credit" --title 26
    """
    archive = AxiomArchive(db_path=ctx.obj["db"])
    results = archive.search(query, title=title, limit=limit)

    if not results:
        console.print(f"[yellow]No results for:[/yellow] {query}")
        return

    table = Table(title=f"Search: {query}")
    table.add_column("Citation", style="cyan")
    table.add_column("Title", style="green")
    table.add_column("Snippet")
    table.add_column("Score", justify="right")

    for r in results:
        table.add_row(
            r.citation.usc_cite,
            r.section_title[:40] + "..." if len(r.section_title) > 40 else r.section_title,
            r.snippet[:60] + "..." if len(r.snippet) > 60 else r.snippet,
            f"{r.score:.2f}",
        )

    console.print(table)


@main.command()
@click.pass_context
def titles(ctx: click.Context):
    """List all available titles."""
    archive = AxiomArchive(db_path=ctx.obj["db"])
    title_list = archive.list_titles()

    if not title_list:
        console.print("[yellow]No titles loaded. Use 'axiom ingest' to add titles.[/yellow]")
        return

    table = Table(title="US Code Titles")
    table.add_column("Title", justify="right", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Sections", justify="right")
    table.add_column("Positive Law", justify="center")
    table.add_column("Updated")

    for t in title_list:
        table.add_row(
            str(t.number),
            t.name,
            str(t.section_count),
            "[green]Yes[/green]" if t.is_positive_law else "[dim]No[/dim]",
            t.last_updated.isoformat(),
        )

    console.print(table)


@main.command()
@click.argument("xml_path", type=click.Path(exists=True, path_type=Path))
@click.pass_context
def ingest(ctx: click.Context, xml_path: Path):
    """Ingest a US Code title from USLM XML file.

    Example:
        axiom ingest data/uscode/usc26.xml
    """
    archive = AxiomArchive(db_path=ctx.obj["db"])
    with console.status(f"Ingesting {xml_path}..."):
        count = archive.ingest_title(xml_path)
    console.print(f"[green]Successfully ingested {count} sections[/green]")


@main.command()
@click.argument("title_num", type=int)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("data/uscode"),
    help="Output directory",
)
def download(title_num: int, output: Path):
    """Download a US Code title from uscode.house.gov.

    Example:
        axiom-corpus download 26 -o data/uscode
    """
    with console.status(f"Downloading Title {title_num}..."):
        path = download_title(title_num, output)
    console.print(f"[green]Downloaded to {path}[/green]")


@main.command()
@click.option("--host", default="127.0.0.1", help="Host to bind")
@click.option("--port", default=8000, help="Port to bind")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
@click.pass_context
def serve(ctx: click.Context, host: str, port: int, reload: bool):
    """Start the REST API server.

    Example:
        axiom-corpus serve --host 0.0.0.0 --port 8080
    """
    import uvicorn

    console.print(f"[green]Starting server at http://{host}:{port}[/green]")
    console.print(f"[dim]API docs at http://{host}:{port}/docs[/dim]")

    # We need to pass the db path to the app
    # For now, use environment variable or default
    uvicorn.run(
        "axiom.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )


@main.command()
@click.argument("citation")
@click.pass_context
def refs(ctx: click.Context, citation: str):
    """Show cross-references for a section.

    Example:
        axiom refs "26 USC 32"
    """
    archive = AxiomArchive(db_path=ctx.obj["db"])
    refs = archive.get_references(citation)

    console.print(
        Panel(
            f"[bold]References from {citation}:[/bold]\n"
            + "\n".join(f"  → {r}" for r in refs["references_to"])
            or "  (none)"
            + "\n\n[bold]Referenced by:[/bold]\n"
            + "\n".join(f"  ← {r}" for r in refs["referenced_by"])
            or "  (none)",
            title=f"Cross-references: {citation}",
        )
    )


@main.command()
@click.argument("citation")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.home() / ".rulespec" / "workspace",
    help="Output directory for encoded files",
)
@click.option(
    "--model", "-m", default="claude-sonnet-4-20250514", help="Claude model to use for encoding"
)
@click.pass_context
def encode(ctx: click.Context, citation: str, output: Path, model: str):
    """Encode a statute section into RuleSpec using AI.

    Reads the statute from the archive and generates:
    - rules.yaml (DSL code)
    - tests.yaml (test cases)
    - statute.md (reference text)
    - metadata.json (provenance)

    Examples:
        axiom encode "26 USC 32"
        axiom encode "26 USC 24" -o ./my-workspace
    """
    from axiom.encoder import encode_and_save
    from axiom.models import Citation

    archive = AxiomArchive(db_path=ctx.obj["db"])

    # Parse citation
    try:
        parsed = Citation.from_string(citation)
    except ValueError as e:
        console.print(f"[red]Invalid citation:[/red] {e}")
        raise SystemExit(1) from e

    # Get section
    section = archive.storage.get_section(parsed.title, parsed.section)
    if not section:
        console.print(f"[red]Section not found:[/red] {citation}")
        raise SystemExit(1)

    console.print(f"[blue]Encoding:[/blue] {citation}")
    console.print(f"[dim]Title: {section.section_title}[/dim]")
    console.print(
        f"[dim]Text: {len(section.text)} chars, {len(section.subsections)} subsections[/dim]"
    )
    console.print()

    with console.status(f"Generating DSL with {model}..."):
        result = encode_and_save(section, output, model=model)

    section_dir = output / "federal" / "statute" / str(parsed.title) / parsed.section

    console.print("[green]✓ Encoding complete![/green]")
    console.print()
    console.print("[bold]Files created:[/bold]")
    console.print(f"  📜 {section_dir / 'statute.md'}")
    console.print(f"  📄 {section_dir / 'rules.yaml'}")
    console.print(f"  🧪 {section_dir / 'tests.yaml'}")
    console.print(f"  📋 {section_dir / 'metadata.json'}")
    console.print()
    console.print(f"[dim]Model: {result.model}[/dim]")
    console.print(f"[dim]Tokens: {result.prompt_tokens} in, {result.completion_tokens} out[/dim]")


@main.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
def validate(path: Path):
    """Validate a local encoding.

    Checks:
    - DSL syntax
    - Parameter references
    - Test case format

    Example:
        axiom validate ~/.rulespec/workspace/federal/statute/26/32
    """
    # Find rules.yaml file
    rules_file = path / "rules.yaml" if path.is_dir() else path

    if not rules_file.exists():
        console.print(f"[red]Not found:[/red] {rules_file}")
        raise SystemExit(1)

    content = rules_file.read_text()

    # Basic validation
    errors = []
    warnings = []

    # Check for required elements
    if "variable " not in content and "parameter " not in content:
        errors.append("No variable or parameter definitions found")

    if "reference " not in content:
        warnings.append("No statute references found")

    if "formula {" not in content:
        warnings.append("No formulas found - is this just parameters?")

    # Count definitions
    var_count = content.count("variable ")
    param_count = content.count("parameter ")
    ref_count = content.count('reference "')

    if errors:
        console.print("[red]✗ Validation failed[/red]")
        for e in errors:
            console.print(f"  [red]ERROR:[/red] {e}")
        raise SystemExit(1)

    console.print("[green]✓ Validation passed[/green]")
    console.print(f"  Variables: {var_count}")
    console.print(f"  Parameters: {param_count}")
    console.print(f"  References: {ref_count}")

    if warnings:
        console.print()
        for w in warnings:
            console.print(f"  [yellow]WARNING:[/yellow] {w}")


@main.command("download-state")
@click.argument("state", type=click.Choice(["ny", "fl", "tx"], case_sensitive=False))
@click.option(
    "--law",
    "-l",
    multiple=True,
    help="Law code(s) to download (e.g., TAX, SOS for NY; chapter numbers for FL).",
)
@click.option(
    "--list-laws",
    is_flag=True,
    help="List available law codes for the state",
)
@click.pass_context
def download_state(ctx: click.Context, state: str, law: tuple[str, ...], list_laws: bool):
    """Download state statutes from official APIs.

    Currently supported states:
    - ny: New York (requires NY_LEGISLATION_API_KEY env var)
    - fl: Florida (web scraping, no API key needed)
    - tx: Texas (bulk ZIP download, no API key needed)

    Examples:
        axiom-corpus download-state ny                    # Download TAX and SOS laws
        axiom-corpus download-state ny --law TAX          # Download only Tax Law
        axiom-corpus download-state ny --list-laws        # List available law codes
        axiom-corpus download-state fl                    # Download FL tax chapters
        axiom-corpus download-state fl --law 212          # Download specific chapter
        axiom-corpus download-state tx                    # Download TX priority codes
        axiom-corpus download-state tx --law TX           # Download just Tax Code
    """
    if state.lower() == "ny":
        _download_ny_state(ctx, law, list_laws)
    elif state.lower() == "fl":
        _download_fl_state(ctx, law, list_laws)
    elif state.lower() == "tx":
        _download_tx_state(ctx, law, list_laws)
    else:
        console.print(f"[red]State not supported:[/red] {state}")
        raise SystemExit(1)


def _download_ny_state(ctx: click.Context, law_codes: tuple[str, ...], list_laws: bool) -> None:
    """Download New York state statutes."""
    import os

    from axiom.parsers.us_ny.statutes import NY_LAW_CODES, NYLegislationClient, download_ny_law

    # Check for API key
    if not os.environ.get("NY_LEGISLATION_API_KEY"):
        console.print("[red]Error:[/red] NY_LEGISLATION_API_KEY environment variable not set.")
        console.print("\nTo get a free API key:")
        console.print("  1. Visit https://legislation.nysenate.gov")
        console.print("  2. Register for an account")
        console.print("  3. Copy your API key")
        console.print("  4. Set: export NY_LEGISLATION_API_KEY=your_key_here")
        raise SystemExit(1)

    # List laws mode
    if list_laws:
        try:
            with NYLegislationClient() as client:
                laws = client.get_law_ids()

            table = Table(title="New York State Laws")
            table.add_column("Code", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Type")

            for law in sorted(laws, key=lambda x: x.law_id):
                table.add_row(law.law_id, law.name, law.law_type)

            console.print(table)
            console.print(f"\n[dim]Total: {len(laws)} laws available[/dim]")
        except Exception as e:
            console.print(f"[red]Error listing laws:[/red] {e}")
            raise SystemExit(1) from e
        return

    # Default to TAX and SOS if no laws specified
    laws_to_download = list(law_codes) if law_codes else ["TAX", "SOS"]

    archive = AxiomArchive(db_path=ctx.obj["db"])
    total_sections = 0

    for law_id in laws_to_download:
        law_name = NY_LAW_CODES.get(law_id.upper(), f"{law_id} Law")
        console.print(f"\n[blue]Downloading:[/blue] New York {law_name} ({law_id})")

        try:
            count = 0
            with console.status(f"Fetching {law_id}..."):
                for section in download_ny_law(law_id.upper()):
                    archive.storage.store_section(section)
                    count += 1
                    if count % 50 == 0:
                        console.print(f"  [dim]Processed {count} sections...[/dim]")

            console.print(f"[green]Stored {count} sections from {law_id}[/green]")
            total_sections += count

        except Exception as e:
            console.print(f"[red]Error downloading {law_id}:[/red] {e}")
            continue

    console.print(f"\n[green]Total: {total_sections} sections stored[/green]")


def _download_fl_state(ctx: click.Context, chapters: tuple[str, ...], list_laws: bool) -> None:
    """Download Florida state statutes."""
    from axiom.parsers.us_fl.statutes import (
        FL_TAX_CHAPTERS,
        FL_WELFARE_CHAPTERS,
        FLStatutesClient,
        convert_to_section,
    )

    # List chapters mode
    if list_laws:
        table = Table(title="Florida Statutes Chapters")
        table.add_column("Chapter", style="cyan")
        table.add_column("Title", style="green")
        table.add_column("Category")

        for ch, title in sorted(FL_TAX_CHAPTERS.items()):
            table.add_row(str(ch), title, "Tax & Finance")

        for ch, title in sorted(FL_WELFARE_CHAPTERS.items()):
            table.add_row(str(ch), title, "Social Welfare")

        console.print(table)
        console.print(
            f"\n[dim]Total: {len(FL_TAX_CHAPTERS) + len(FL_WELFARE_CHAPTERS)} chapters available[/dim]"
        )
        return

    # Default to tax chapters if none specified
    chapter_list = [int(ch) for ch in chapters] if chapters else list(FL_TAX_CHAPTERS.keys())

    archive = AxiomArchive(db_path=ctx.obj["db"])
    total_sections = 0

    with FLStatutesClient(rate_limit_delay=0.3) as client:
        for chapter in chapter_list:
            chapter_name = FL_TAX_CHAPTERS.get(chapter) or FL_WELFARE_CHAPTERS.get(
                chapter, f"Chapter {chapter}"
            )
            console.print(f"\n[blue]Downloading:[/blue] Florida {chapter_name} (Ch. {chapter})")

            try:
                count = 0
                with console.status(f"Fetching chapter {chapter}..."):
                    for fl_section in client.iter_chapter(chapter):
                        section = convert_to_section(fl_section)
                        archive.storage.store_section(section)
                        count += 1
                        if count % 20 == 0:
                            console.print(f"  [dim]Processed {count} sections...[/dim]")

                console.print(f"[green]Stored {count} sections from Chapter {chapter}[/green]")
                total_sections += count

            except Exception as e:
                console.print(f"[red]Error downloading Chapter {chapter}:[/red] {e}")
                continue

    console.print(f"\n[green]Total: {total_sections} sections stored[/green]")


def _download_tx_state(ctx: click.Context, codes: tuple[str, ...], list_laws: bool) -> None:
    """Download Texas state statutes."""
    from axiom.parsers.us_tx.statutes import (
        TX_CODES,
        TX_PRIORITY_CODES,
        TXStatutesClient,
        convert_to_section,
    )

    # List codes mode
    if list_laws:
        table = Table(title="Texas Statutes Codes")
        table.add_column("Code", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Priority")

        for code, name in sorted(TX_CODES.items()):
            priority = "Yes" if code in TX_PRIORITY_CODES else ""
            table.add_row(code, name, priority)

        console.print(table)
        console.print(f"\n[dim]Total: {len(TX_CODES)} codes available[/dim]")
        console.print("[dim]Priority codes are downloaded by default[/dim]")
        return

    # Default to priority codes if none specified
    code_list = [c.upper() for c in codes] if codes else TX_PRIORITY_CODES

    archive = AxiomArchive(db_path=ctx.obj["db"])
    total_sections = 0

    with TXStatutesClient() as client:
        for code in code_list:
            code_name = TX_CODES.get(code, f"{code} Code")
            console.print(f"\n[blue]Downloading:[/blue] Texas {code_name}")

            try:
                count = 0
                with console.status(f"Downloading and parsing {code}..."):
                    for tx_section in client.iter_code(code):
                        section = convert_to_section(tx_section)
                        archive.storage.store_section(section)
                        count += 1
                        if count % 100 == 0:
                            console.print(f"  [dim]Processed {count} sections...[/dim]")

                console.print(f"[green]Stored {count} sections from {code}[/green]")
                total_sections += count

            except Exception as e:
                console.print(f"[red]Error downloading {code}:[/red] {e}")
                continue

    console.print(f"\n[green]Total: {total_sections} sections stored[/green]")


@main.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--pe-var",
    "-v",
    required=True,
    help="PolicyEngine variable name to compare (e.g., 'eitc', 'ctc')",
)
@click.option("--tolerance", "-t", default=15.0, help="Dollar tolerance for matching (default $15)")
@click.option("--save", "-s", type=click.Path(path_type=Path), help="Save report to JSON file")
def verify(path: Path, pe_var: str, tolerance: float, save: Path | None):
    """Verify a DSL encoding against PolicyEngine API.

    Runs test cases through PolicyEngine's API and compares results
    to expected values from the DSL encoding.

    Examples:
        axiom verify ~/.rulespec/workspace/federal/statute/26/32 -v eitc
        axiom verify ~/.rulespec/workspace/federal/statute/26/24 -v ctc
    """
    from axiom.verifier import (
        print_verification_report,
        save_verification_report,
        verify_encoding,
    )

    section_dir = path if path.is_dir() else path.parent

    with console.status(f"Verifying against PolicyEngine API ({pe_var})..."):
        report = verify_encoding(section_dir, pe_var, tolerance)

    print_verification_report(report)

    if save:
        save_verification_report(report, save)
        console.print(f"\n[dim]Report saved to {save}[/dim]")


@main.command("fetch-guidance")
@click.option(
    "--year",
    "-y",
    type=int,
    multiple=True,
    help="Year(s) to fetch (e.g., 2024). Can be repeated. Default: 2020-2024",
)
@click.option(
    "--type",
    "-t",
    "doc_types",
    type=click.Choice(["rev-proc", "rev-rul", "notice", "all"], case_sensitive=False),
    multiple=True,
    default=["all"],
    help="Document type(s) to fetch. Default: all",
)
@click.option(
    "--download-pdfs",
    is_flag=True,
    help="Also download PDF files to data/guidance/irs/",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="List documents without fetching",
)
@click.pass_context
def fetch_guidance(
    ctx: click.Context,
    year: tuple[int, ...],
    doc_types: tuple[str, ...],
    download_pdfs: bool,
    dry_run: bool,
):
    """Fetch IRS guidance documents (Rev. Procs, Rev. Rulings, Notices).

    Downloads documents from https://www.irs.gov/pub/irs-drop/ and stores
    metadata in the database. By default fetches all document types for
    years 2020-2024.

    Examples:
        axiom fetch-guidance                     # Fetch all 2020-2024
        axiom fetch-guidance --year 2024         # Just 2024
        axiom fetch-guidance -y 2023 -y 2024     # 2023 and 2024
        axiom fetch-guidance --type rev-proc     # Only Revenue Procedures
        axiom fetch-guidance --dry-run           # List without fetching
    """
    # Parse years
    years = list(year) if year else [2020, 2021, 2022, 2023, 2024]

    # Parse document types
    type_mapping = {
        "rev-proc": [GuidanceType.REV_PROC],
        "rev-rul": [GuidanceType.REV_RUL],
        "notice": [GuidanceType.NOTICE],
        "all": [GuidanceType.REV_PROC, GuidanceType.REV_RUL, GuidanceType.NOTICE],
    }

    selected_types = []
    for dt in doc_types:
        selected_types.extend(type_mapping[dt.lower()])
    selected_types = list(set(selected_types))

    console.print(f"[blue]Fetching IRS guidance for years:[/blue] {years}")
    console.print(f"[blue]Document types:[/blue] {[t.value for t in selected_types]}")

    if dry_run:
        console.print("\n[yellow]DRY RUN - listing documents only[/yellow]\n")

    # Initialize storage and fetcher
    storage = GuidanceStorage(ctx.obj["db"])
    download_dir = Path("data/guidance/irs") if download_pdfs else None

    fetched_count = 0
    error_count = 0

    with IRSBulkFetcher() as fetcher:
        # First, list all available documents (multi-page)
        console.print(
            "[dim]Scanning IRS drop folder (may take a minute for multiple pages)...[/dim]"
        )

        def page_progress(msg: str) -> None:
            console.print(f"[dim]{msg}[/dim]")

        html = fetcher._fetch_drop_listing(progress_callback=page_progress)
        from axiom.fetchers.irs_bulk import parse_irs_drop_listing

        all_docs = []
        for y in years:
            docs = parse_irs_drop_listing(html, year=y, doc_types=selected_types)
            all_docs.extend(docs)

        console.print(f"[green]Found {len(all_docs)} documents[/green]\n")

        if dry_run:
            # Just show a table of documents
            table = Table(title="Available IRS Guidance Documents")
            table.add_column("Type", style="cyan")
            table.add_column("Number", style="green")
            table.add_column("Year", justify="right")
            table.add_column("PDF URL")

            for doc in sorted(all_docs, key=lambda d: (d.year, d.doc_type.value, d.doc_number)):
                table.add_row(
                    doc.doc_type.value,
                    doc.doc_number,
                    str(doc.year),
                    doc.pdf_url,
                )

            console.print(table)
            return

        # Fetch and store each document
        for i, doc in enumerate(all_docs):
            console.print(
                f"[{i + 1}/{len(all_docs)}] Fetching {doc.doc_type.value} {doc.doc_number}...",
                end=" ",
            )

            try:
                # Fetch PDF to get file size (we don't parse content yet)
                pdf_content = fetcher.fetch_pdf(doc)
                pdf_size = len(pdf_content)

                # Optionally save PDF
                if download_dir:
                    download_dir.mkdir(parents=True, exist_ok=True)
                    pdf_path = download_dir / doc.pdf_filename
                    pdf_path.write_bytes(pdf_content)

                # Create RevenueProcedure model with placeholder content
                from datetime import date as date_module

                from axiom.models_guidance import RevenueProcedure

                rev_proc = RevenueProcedure(
                    doc_number=doc.doc_number,
                    doc_type=doc.doc_type,
                    title=fetcher._generate_title(doc),
                    irb_citation="",  # Would need IRB lookup
                    published_date=date_module(doc.year, 1, 1),  # Placeholder
                    full_text=f"[PDF content: {pdf_size} bytes]",
                    sections=[],
                    effective_date=None,
                    tax_years=[doc.year, doc.year + 1],
                    subject_areas=["General"],
                    parameters={},
                    source_url=doc.pdf_url,
                    pdf_url=doc.pdf_url,
                    retrieved_at=date_module.today(),
                )

                # Store in database
                storage.store_revenue_procedure(rev_proc)
                fetched_count += 1
                console.print(f"[green]OK[/green] ({pdf_size:,} bytes)")

            except Exception as e:
                error_count += 1
                console.print(f"[red]ERROR: {e}[/red]")

    console.print()
    console.print(f"[green]Successfully fetched:[/green] {fetched_count} documents")
    if error_count:
        console.print(f"[red]Errors:[/red] {error_count}")

    # Show final count in database
    total = storage.db.execute("SELECT COUNT(*) FROM guidance_documents").fetchone()[0]
    console.print(f"\n[dim]Total documents in database: {total}[/dim]")


@main.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def stats(ctx: click.Context, as_json: bool):
    """Show archive statistics and scraping progress.

    Displays counts of:
    - US Code titles and sections
    - State statutes by state
    - IRS guidance by type and year
    - Database size and storage info

    Example:
        axiom-corpus stats
        axiom-corpus stats --json
    """
    import json as json_module
    from datetime import date

    archive = AxiomArchive(db_path=ctx.obj["db"])
    db = archive.storage.db

    stats_data: dict = {
        "generated_at": date.today().isoformat(),
        "database": ctx.obj["db"],
        "us_code": {},
        "state_statutes": {},
        "irs_guidance": {},
        "totals": {},
    }

    # US Code statistics
    usc_query = """
        SELECT title, title_name, COUNT(*) as section_count,
               MAX(retrieved_at) as last_updated
        FROM sections
        WHERE title > 0
        GROUP BY title
        ORDER BY title
    """
    usc_rows = db.execute(usc_query).fetchall()

    usc_stats = []
    usc_total_sections = 0
    for row in usc_rows:
        usc_stats.append(
            {
                "title": row[0],
                "name": row[1],
                "sections": row[2],
                "last_updated": row[3],
            }
        )
        usc_total_sections += row[2]

    stats_data["us_code"] = {
        "titles": len(usc_stats),
        "total_sections": usc_total_sections,
        "by_title": usc_stats,
    }

    # State statutes (title 0 or negative titles are state codes)
    state_query = """
        SELECT
            CASE
                WHEN section LIKE 'NY-%' THEN 'NY'
                WHEN section LIKE 'CA-%' THEN 'CA'
                ELSE 'Other'
            END as state,
            COUNT(*) as section_count
        FROM sections
        WHERE title = 0 OR title < 0
        GROUP BY state
        ORDER BY state
    """
    state_rows = db.execute(state_query).fetchall()

    state_stats = {}
    state_total = 0
    for row in state_rows:
        state_stats[row[0]] = row[1]
        state_total += row[1]

    # Get NY law breakdown
    ny_breakdown_query = """
        SELECT
            SUBSTR(section, 4, INSTR(SUBSTR(section, 4), '-') - 1) as law_code,
            COUNT(*) as sections
        FROM sections
        WHERE section LIKE 'NY-%'
        GROUP BY law_code
        ORDER BY sections DESC
    """
    ny_rows = db.execute(ny_breakdown_query).fetchall()
    ny_breakdown = {row[0]: row[1] for row in ny_rows}

    stats_data["state_statutes"] = {
        "total_sections": state_total,
        "by_state": state_stats,
        "ny_breakdown": ny_breakdown,
    }

    # IRS Guidance statistics
    guidance_query = """
        SELECT doc_type, COUNT(*) as count,
               MIN(SUBSTR(doc_number, 1, 4)) as earliest_year,
               MAX(SUBSTR(doc_number, 1, 4)) as latest_year
        FROM guidance_documents
        GROUP BY doc_type
    """
    try:
        guidance_rows = db.execute(guidance_query).fetchall()
        guidance_stats = {}
        guidance_total = 0
        for row in guidance_rows:
            guidance_stats[row[0]] = {
                "count": row[1],
                "year_range": f"{row[2]}-{row[3]}",
            }
            guidance_total += row[1]

        stats_data["irs_guidance"] = {
            "total_documents": guidance_total,
            "by_type": guidance_stats,
        }
    except Exception:
        stats_data["irs_guidance"] = {"total_documents": 0, "by_type": {}}

    # Totals
    stats_data["totals"] = {
        "usc_titles": len(usc_stats),
        "usc_sections": usc_total_sections,
        "state_sections": state_total,
        "guidance_documents": stats_data["irs_guidance"].get("total_documents", 0),
        "all_sections": usc_total_sections + state_total,
    }

    # Database file size
    db_path = Path(ctx.obj["db"])
    if db_path.exists():
        stats_data["database_size_mb"] = round(db_path.stat().st_size / 1024 / 1024, 2)

    if as_json:
        console.print_json(json_module.dumps(stats_data, indent=2))
        return

    # Pretty print
    console.print()
    console.print(
        Panel(
            "[bold cyan]Axiom - Archive Statistics[/bold cyan]",
            subtitle=f"Database: {ctx.obj['db']}",
        )
    )

    # US Code table
    table = Table(title="📜 US Code", show_header=True, header_style="bold cyan")
    table.add_column("Title", justify="right", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Sections", justify="right", style="green")

    for t in stats_data["us_code"]["by_title"][:10]:  # Top 10
        table.add_row(str(t["title"]), t["name"][:35], f"{t['sections']:,}")

    if len(stats_data["us_code"]["by_title"]) > 10:
        table.add_row("...", f"... and {len(stats_data['us_code']['by_title']) - 10} more", "")

    table.add_row("", "[bold]TOTAL[/bold]", f"[bold]{usc_total_sections:,}[/bold]")
    console.print(table)

    # State statutes
    if state_total > 0:
        console.print()
        table2 = Table(title="🏛️  State Statutes", show_header=True, header_style="bold cyan")
        table2.add_column("State", style="cyan")
        table2.add_column("Law", style="white")
        table2.add_column("Sections", justify="right", style="green")

        for law, count in sorted(ny_breakdown.items(), key=lambda x: -x[1]):
            table2.add_row("NY", law, f"{count:,}")

        table2.add_row("", "[bold]TOTAL[/bold]", f"[bold]{state_total:,}[/bold]")
        console.print(table2)

    # IRS Guidance
    if stats_data["irs_guidance"].get("total_documents", 0) > 0:
        console.print()
        table3 = Table(title="📋 IRS Guidance", show_header=True, header_style="bold cyan")
        table3.add_column("Type", style="cyan")
        table3.add_column("Count", justify="right", style="green")
        table3.add_column("Years", style="dim")

        for doc_type, info in stats_data["irs_guidance"]["by_type"].items():
            table3.add_row(doc_type, str(info["count"]), info["year_range"])

        table3.add_row(
            "[bold]TOTAL[/bold]",
            f"[bold]{stats_data['irs_guidance']['total_documents']}[/bold]",
            "",
        )
        console.print(table3)

    # Summary
    console.print()
    console.print(
        Panel(
            f"[bold green]Total Sections:[/bold green] {stats_data['totals']['all_sections']:,}\n"
            f"[bold blue]US Code Titles:[/bold blue] {stats_data['totals']['usc_titles']}\n"
            f"[bold yellow]IRS Documents:[/bold yellow] {stats_data['totals']['guidance_documents']}\n"
            f"[dim]Database Size: {stats_data.get('database_size_mb', 'N/A')} MB[/dim]",
            title="Summary",
        )
    )


@main.command()
@click.argument("jurisdiction", default="all")
@click.option("--output", "-o", default="data/statutes", help="Output directory")
@click.option("--dry-run", is_flag=True, help="Show what would be crawled without fetching")
@click.option("--max-sections", "-n", type=int, help="Limit sections per jurisdiction")
@click.option("--concurrency", "-c", default=5, help="Concurrent requests (HTML only)")
@click.option("--force", "-f", is_flag=True, help="Re-download even if files exist")
@click.option(
    "--min-files", default=10, help="Skip states with at least this many files (default: 10)"
)
@click.option("--log", "-l", is_flag=True, help="Write log to timestamped file in data/")
def crawl(
    jurisdiction: str,
    output: str,
    dry_run: bool,
    max_sections: int | None,
    concurrency: int,
    force: bool,
    min_files: int,
    log: bool,
):
    """Crawl statutes from official sources.

    JURISDICTION can be:
      - 'all': Crawl all configured states
      - 'us': Federal US Code only
      - 'us-ca': California
      - 'us-tx': Texas
      - etc.

    By default, skips states that already have >= 10 files downloaded.
    Use --force to re-crawl, or --min-files to adjust threshold.

    Examples:
        axiom crawl all                    # Crawl all, skip existing
        axiom crawl all --force            # Re-crawl everything
        axiom crawl all --min-files 50     # Skip states with 50+ files
        axiom crawl us-ca --dry-run        # Preview California crawl
        axiom crawl us-tx -n 100           # Texas, first 100 sections
    """
    import asyncio
    import sys
    from datetime import datetime

    from axiom.crawl import (
        ARCHIVE_ORG_STATES,
        crawl_jurisdiction,
        download_from_archive_org,
        get_all_jurisdictions,
    )
    from axiom.sources.specs import is_archive_org_state, is_playwright_state

    # Setup logging to timestamped file
    log_file = None
    if log:
        log_dir = Path("data")
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"crawl_{timestamp}.log"
        log_file = open(log_path, "w")  # noqa: SIM115 - closed after stdout is restored.
        console.print(f"[cyan]Logging to: {log_path}[/cyan]")

        # Redirect stdout to both console and file
        class TeeWriter:
            def __init__(self, *writers):
                self.writers = writers

            def write(self, text):
                for w in self.writers:
                    w.write(text)
                    w.flush()

            def flush(self):
                for w in self.writers:
                    w.flush()

        sys.stdout = TeeWriter(sys.__stdout__, log_file)

    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which jurisdictions to crawl
    if jurisdiction == "all":
        jurisdictions = get_all_jurisdictions()
    elif jurisdiction == "us":
        # Federal only - handled separately via USLM
        console.print("[cyan]Federal US Code uses USLM parser, not web crawl.[/cyan]")
        console.print("Use: [bold]axiom-corpus download <title>[/bold] for federal titles")
        return
    else:
        jurisdictions = [jurisdiction]

    # Playwright-required states (fallback if no spec)
    playwright_states_fallback = {"us-al", "us-ak", "us-tx"}

    async def run_crawls():
        results = {}
        total = len(jurisdictions)
        skipped = 0

        for i, jur in enumerate(jurisdictions, 1):
            progress = f"[{i}/{total}]"

            # Check if already has enough files (skip unless --force)
            jur_dir = output_dir / jur
            if not force and jur_dir.exists():
                existing = len(list(jur_dir.glob("*.html")))
                if existing >= min_files:
                    console.print(f"\n[dim]{progress} Skip {jur} ({existing} files exist)[/dim]")
                    results[jur] = {"source": "skipped", "files": existing, "skipped": True}
                    skipped += 1
                    continue

            console.print(
                f"\n[bold blue]{progress} {'[DRY RUN] ' if dry_run else ''}Crawling {jur}...[/bold blue]"
            )

            # Check if Archive.org has bulk data (spec first, then fallback)
            if is_archive_org_state(jur) or jur in ARCHIVE_ORG_STATES:
                console.print("  [green]→ Using Archive.org bulk download[/green]")
                if not dry_run:
                    result = await download_from_archive_org(
                        jur, output_dir=output_dir / jur, dry_run=dry_run
                    )
                    results[jur] = result
                else:
                    results[jur] = {"source": "archive.org", "dry_run": True}
                continue

            # Check if Playwright is needed (spec first, then fallback)
            if is_playwright_state(jur) or jur in playwright_states_fallback:
                console.print("  [yellow]→ Using Playwright (JavaScript SPA)[/yellow]")
                if not dry_run:
                    try:
                        from axiom.crawl_playwright import crawl_state

                        result = await crawl_state(
                            jur.replace("us-", ""),
                            output_dir=output_dir / jur,
                            max_sections=max_sections,
                        )
                        results[jur] = result
                    except Exception as e:
                        console.print(f"  [red]Error: {e}[/red]")
                        results[jur] = {"error": str(e)}
                else:
                    results[jur] = {"source": "playwright", "dry_run": True}
                continue

            # Standard HTML crawler
            console.print("  [cyan]→ Using async HTML crawler[/cyan]")
            if not dry_run:
                try:
                    result = await crawl_jurisdiction(
                        jur,
                        output_dir=output_dir / jur,
                        max_sections=max_sections,
                        concurrency=concurrency,
                    )
                    results[jur] = result
                except Exception as e:
                    console.print(f"  [red]Error: {e}[/red]")
                    results[jur] = {"error": str(e)}
            else:
                results[jur] = {"source": "html", "dry_run": True}

        return results

    results = asyncio.run(run_crawls())

    # Summary table
    console.print("\n")
    table = Table(title="Crawl Results")
    table.add_column("Jurisdiction", style="cyan")
    table.add_column("Source", style="green")
    table.add_column("Sections", justify="right")
    table.add_column("Status")

    for jur, result in sorted(results.items()):
        if isinstance(result, dict):
            source = result.get("source", "html")
            sections = result.get("sections", result.get("files", 0))
            if result.get("skipped"):
                status = "[dim]skipped[/dim]"
            elif result.get("error"):
                status = f"[red]{result['error'][:30]}...[/red]"
            elif result.get("dry_run"):
                status = "[yellow]dry run[/yellow]"
            else:
                status = "[green]✓[/green]"
        else:
            source = "unknown"
            sections = 0
            status = "[red]failed[/red]"

        table.add_row(jur, source, str(sections), status)

    console.print(table)

    # Cleanup logging
    if log and log_file:
        sys.stdout = sys.__stdout__
        log_file.close()
        console.print(f"[green]Log saved to: {log_path}[/green]")


@main.command("download-cfr")
@click.argument("title_num", type=int)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.home() / ".axiom" / "cfr",
    help="Output directory",
)
@click.option("--force", "-f", is_flag=True, help="Re-download even if file exists")
def download_cfr(title_num: int, output: Path, force: bool):
    """Download a CFR title from govinfo.gov bulk data.

    Downloads the eCFR XML file for the specified CFR title number.

    Examples:
        axiom-corpus download-cfr 26              # Title 26 (Treasury/IRS)
        axiom-corpus download-cfr 7               # Title 7 (Agriculture/SNAP)
        axiom-corpus download-cfr 26 -o data/cfr  # Custom output directory
    """
    import asyncio

    from axiom.fetchers.ecfr import ECFRFetcher

    fetcher = ECFRFetcher(data_dir=output)

    with console.status(f"Downloading CFR Title {title_num}..."):
        path = asyncio.run(fetcher.download_title(title_num, force=force))

    console.print(f"[green]Downloaded to {path}[/green]")

    # Show file size and section count
    size_mb = path.stat().st_size / 1024 / 1024
    console.print(f"[dim]File size: {size_mb:.1f} MB[/dim]")

    # Quick count of sections
    section_count = fetcher.count_sections(path)
    console.print(f"[dim]Sections: {section_count:,}[/dim]")


@main.command("ingest-cfr")
@click.argument("xml_path", type=click.Path(exists=True, path_type=Path))
@click.option("--parts", "-p", multiple=True, type=int, help="Only ingest specific parts")
@click.pass_context
def ingest_cfr(ctx: click.Context, xml_path: Path, parts: tuple[int, ...]):
    """Ingest a CFR title from downloaded XML file.

    Parses the eCFR XML and stores regulations in the database.

    Examples:
        axiom ingest-cfr ~/.axiom/cfr/title-26.xml
        axiom ingest-cfr data/cfr/title-26.xml --parts 1 31
    """
    from axiom.fetchers.ecfr import ECFRFetcher
    from axiom.storage.regulation import RegulationStorage

    fetcher = ECFRFetcher()
    storage = RegulationStorage(ctx.obj["db"])

    # Get title metadata
    metadata = fetcher.get_title_metadata(xml_path)
    title_num = metadata["title_number"]
    title_name = metadata["title_name"]

    console.print(f"[blue]Ingesting:[/blue] CFR Title {title_num} - {title_name}")

    parts_list = list(parts) if parts else None
    if parts_list:
        console.print(f"[dim]Filtering to parts: {parts_list}[/dim]")

    count = 0
    with console.status(f"Parsing {xml_path.name}..."):
        for regulation in fetcher.parse_title(xml_path, parts=parts_list):
            storage.store_regulation(regulation)
            count += 1
            if count % 100 == 0:
                console.print(f"  [dim]Processed {count:,} regulations...[/dim]")

    # Update title metadata
    storage.update_cfr_title_metadata(
        title_num,
        title_name,
        metadata.get("amendment_date"),
    )

    console.print(f"[green]Successfully ingested {count:,} regulations[/green]")


@main.command("cfr-titles")
@click.pass_context
def cfr_titles(ctx: click.Context):
    """List all ingested CFR titles."""
    from axiom.storage.regulation import RegulationStorage

    storage = RegulationStorage(ctx.obj["db"])
    title_list = storage.list_cfr_titles()

    if not title_list:
        console.print("[yellow]No CFR titles loaded. Use 'axiom ingest-cfr' to add titles.[/yellow]")
        return

    table = Table(title="Code of Federal Regulations")
    table.add_column("Title", justify="right", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Regulations", justify="right")
    table.add_column("Updated")

    for t in title_list:
        table.add_row(
            str(t["number"]),
            t["name"],
            f"{t['regulation_count']:,}",
            t["last_updated"] or "",
        )

    console.print(table)


@main.command("get-cfr")
@click.argument("citation")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def get_cfr(ctx: click.Context, citation: str, as_json: bool):
    """Get a CFR regulation by citation.

    Examples:
        axiom-corpus get-cfr "26 CFR 1.32-1"
        axiom-corpus get-cfr "26 CFR 1.32-1(a)"
        axiom-corpus get-cfr "7 CFR 273.1" --json
    """
    from axiom.models_regulation import CFRCitation
    from axiom.storage.regulation import RegulationStorage

    storage = RegulationStorage(ctx.obj["db"])

    # Parse citation
    try:
        parsed = CFRCitation.from_string(citation)
    except ValueError as e:
        console.print(f"[red]Invalid CFR citation:[/red] {e}")
        raise SystemExit(1) from e

    if parsed.section is None:
        console.print(f"[red]Section number required:[/red] {citation}")
        raise SystemExit(1)

    regulation = storage.get_regulation(parsed.title, parsed.part, parsed.section)

    if not regulation:
        console.print(f"[red]Not found:[/red] {citation}")
        raise SystemExit(1)

    if as_json:
        console.print_json(regulation.model_dump_json())
    else:
        # Format output
        text_preview = regulation.full_text[:2000]
        if len(regulation.full_text) > 2000:
            text_preview += "..."

        console.print(
            Panel(
                f"[bold]{regulation.cfr_cite}[/bold]\n"
                f"[dim]Authority: {regulation.authority}[/dim]\n\n"
                f"[bold blue]{regulation.heading}[/bold blue]\n\n"
                f"{text_preview}\n\n"
                f"[dim]Source: {regulation.source}[/dim]\n"
                f"[dim]Effective: {regulation.effective_date}[/dim]",
                title=regulation.cfr_cite,
            )
        )


@main.command("search-cfr")
@click.argument("query")
@click.option("--title", "-t", type=int, help="Limit to specific CFR title")
@click.option("--limit", "-n", default=10, help="Maximum results")
@click.pass_context
def search_cfr(ctx: click.Context, query: str, title: int | None, limit: int):
    """Search CFR regulations.

    Examples:
        axiom-corpus search-cfr "earned income"
        axiom-corpus search-cfr "food stamps" --title 7
        axiom-corpus search-cfr "withholding" -t 26 -n 20
    """
    from axiom.storage.regulation import RegulationStorage

    storage = RegulationStorage(ctx.obj["db"])
    results = storage.search(query, title=title, limit=limit)

    if not results:
        console.print(f"[yellow]No results for:[/yellow] {query}")
        return

    table = Table(title=f"CFR Search: {query}")
    table.add_column("Citation", style="cyan")
    table.add_column("Heading", style="green")
    table.add_column("Snippet")
    table.add_column("Score", justify="right")

    for r in results:
        heading = r.heading[:35] + "..." if len(r.heading) > 35 else r.heading
        snippet = r.snippet[:50] + "..." if len(r.snippet) > 50 else r.snippet
        table.add_row(r.cfr_cite, heading, snippet, f"{r.score:.2f}")

    console.print(table)


@main.command("download-uk")
@click.argument("citation", required=False)
@click.option("--sections", "-n", type=int, help="Max sections to download per act")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.home() / ".axiom" / "uk",
    help="Output directory",
)
@click.option("--all", "download_all", is_flag=True, help="Download ALL UK Public General Acts")
@click.option("--priority", is_flag=True, help="Download priority acts (tax/benefits)")
@click.option("--list-acts", is_flag=True, help="List all available ukpga acts without downloading")
@click.option("--resume", is_flag=True, help="Resume previous bulk download")
@click.option("--log", "-l", is_flag=True, help="Write progress log to file")
@click.option("--dry-run", is_flag=True, help="Show what would be downloaded without fetching")
def download_uk(
    citation: str | None,
    sections: int | None,
    output: Path,
    download_all: bool,
    priority: bool,
    list_acts: bool,
    resume: bool,
    log: bool,
    dry_run: bool,
):
    """Download UK legislation from legislation.gov.uk.

    CITATION can be:
      - ukpga/2003/1 (entire Act)
      - ukpga/2003/1/section/62 (single section)
      - "ITEPA 2003 s.62" (human-readable)

    Bulk download modes:
      --all       Download ALL UK Public General Acts (~4000 acts)
      --priority  Download priority tax/benefits acts only (10 acts)
      --list-acts List all available acts without downloading

    Examples:
        axiom-corpus download-uk ukpga/2003/1              # Single act (ITEPA 2003)
        axiom-corpus download-uk ukpga/2007/3 -n 50        # ITA 2007, first 50 sections
        axiom-corpus download-uk --priority               # Download priority acts
        axiom-corpus download-uk --all                    # Download ALL ukpga acts
        axiom-corpus download-uk --all --resume           # Resume interrupted download
        axiom-corpus download-uk --list-acts              # List all available acts
    """
    import asyncio
    from datetime import datetime

    from axiom.fetchers.legislation_uk import (
        UK_PRIORITY_ACTS,
        BulkDownloadProgress,
        UKLegislationFetcher,
    )
    from axiom.models_uk import UKCitation

    fetcher = UKLegislationFetcher(data_dir=output)

    # List all acts mode
    if list_acts:
        console.print("[blue]Enumerating all UK Public General Acts...[/blue]")

        async def list_all():
            acts = await fetcher.list_all_ukpga_acts(
                page_size=50,
                progress_callback=lambda msg: console.print(f"[dim]{msg}[/dim]"),
            )
            return acts

        acts = asyncio.run(list_all())

        # Group by year
        by_year: dict[int, list] = {}
        for act in acts:
            by_year.setdefault(act.year, []).append(act)

        table = Table(title=f"UK Public General Acts ({len(acts)} total)")
        table.add_column("Year", style="cyan", justify="right")
        table.add_column("Count", style="green", justify="right")
        table.add_column("Example Acts")

        for year in sorted(by_year.keys(), reverse=True)[:20]:
            year_acts = by_year[year]
            examples = ", ".join(a.title[:30] for a in year_acts[:2])
            if len(year_acts) > 2:
                examples += f" (+{len(year_acts) - 2} more)"
            table.add_row(str(year), str(len(year_acts)), examples)

        console.print(table)
        console.print(
            f"\n[dim]Total: {len(acts)} acts from {min(by_year.keys())} to {max(by_year.keys())}[/dim]"
        )
        return

    # Priority acts mode
    if priority:
        console.print("[blue]Downloading priority UK tax/benefits acts...[/blue]")

        async def download_priority():
            total_sections = 0
            for act_ref in UK_PRIORITY_ACTS:
                try:
                    parsed = UKCitation.from_string(act_ref)
                    console.print(f"\n[cyan]Downloading {act_ref}...[/cyan]")

                    if dry_run:
                        console.print(f"  [yellow]DRY RUN - would download {act_ref}[/yellow]")
                        continue

                    act = await fetcher.fetch_act_metadata(parsed)
                    console.print(f"  [green]{act.title}[/green]")

                    # Download the full XML
                    act_output = output / "ukpga" / str(parsed.year) / f"{parsed.number}.xml"
                    act_output.parent.mkdir(parents=True, exist_ok=True)

                    url = f"https://www.legislation.gov.uk/{act_ref}/data.xml"
                    await fetcher._rate_limit()

                    import httpx

                    async with httpx.AsyncClient() as client:
                        response = await client.get(
                            url,
                            headers={"User-Agent": "Axiom/1.0"},
                            follow_redirects=True,
                            timeout=120,
                        )
                        response.raise_for_status()
                        act_output.write_text(response.text)

                    section_count = response.text.count("<P1 ")
                    total_sections += section_count
                    console.print(f"  [dim]Saved {section_count} sections to {act_output}[/dim]")

                except Exception as e:
                    console.print(f"  [red]FAILED: {e}[/red]")

            return total_sections

        total = asyncio.run(download_priority())
        console.print(
            f"\n[green]Downloaded {len(UK_PRIORITY_ACTS)} priority acts ({total} total sections)[/green]"
        )
        return

    # Bulk download ALL acts mode
    if download_all:
        console.print("[bold blue]Bulk downloading ALL UK Public General Acts[/bold blue]")
        console.print(f"[dim]Output directory: {output}[/dim]")

        if dry_run:
            console.print("\n[yellow]DRY RUN - enumerating acts only[/yellow]")

        # Setup log file
        log_file = None
        if log:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = output / "logs" / f"download_{timestamp}.log"
            console.print(f"[dim]Log file: {log_file}[/dim]")

        # Setup progress tracker
        progress_file = output / "ukpga" / "progress.json"
        progress = BulkDownloadProgress(progress_file)

        if resume and progress.downloaded:
            console.print(f"[cyan]Resuming download: {progress.summary}[/cyan]")
        elif progress.downloaded and not resume:
            console.print(
                f"[yellow]Previous progress found ({len(progress.downloaded)} acts)[/yellow]"
            )
            console.print(
                "[yellow]Use --resume to continue, or delete progress.json to start fresh[/yellow]"
            )
            return

        async def bulk_download():
            if dry_run:
                # Just enumerate
                acts = await fetcher.list_all_ukpga_acts(
                    page_size=50,
                    progress_callback=lambda msg: console.print(f"[dim]{msg}[/dim]"),
                )
                console.print(
                    f"\n[green]Found {len(acts)} acts (dry run - not downloading)[/green]"
                )
                return None

            result = await fetcher.bulk_download_ukpga(
                output_dir=output / "ukpga",
                progress=progress,
                progress_callback=lambda msg: console.print(msg),
                log_file=log_file,
            )
            return result

        result = asyncio.run(bulk_download())

        if result:
            console.print("\n[bold green]Download complete![/bold green]")
            console.print(f"[green]{result.summary}[/green]")

            if result.failed:
                console.print(f"\n[yellow]Failed acts ({len(result.failed)}):[/yellow]")
                for act_id, error in list(result.failed.items())[:10]:
                    console.print(f"  [red]{act_id}:[/red] {error}")
                if len(result.failed) > 10:
                    console.print(f"  [dim]... and {len(result.failed) - 10} more[/dim]")

        return

    # Single act/section download (original behavior)
    if not citation:
        console.print(
            "[red]Error:[/red] CITATION required unless using --all, --priority, or --list-acts"
        )
        console.print("\nExamples:")
        console.print("  axiom-corpus download-uk ukpga/2003/1")
        console.print("  axiom-corpus download-uk --all")
        console.print("  axiom-corpus download-uk --priority")
        raise SystemExit(1)

    # Parse citation
    try:
        parsed = UKCitation.from_string(citation)
    except ValueError as e:
        console.print(f"[red]Invalid citation:[/red] {e}")
        raise SystemExit(1) from e

    if parsed.section:
        # Single section
        console.print(f"[blue]Fetching:[/blue] {parsed.legislation_url}")
        with console.status("Downloading..."):
            section = asyncio.run(fetcher.fetch_section(parsed))
        console.print(f"[green]Downloaded:[/green] {section.title}")
        console.print(f"[dim]Text: {len(section.text)} chars[/dim]")
    else:
        # Entire Act
        console.print(f"[blue]Fetching Act:[/blue] {parsed.legislation_url}")

        async def fetch_all():
            act = await fetcher.fetch_act_metadata(parsed)
            console.print(f"[green]Act:[/green] {act.title}")
            console.print(f"[dim]Sections: {act.section_count or 'unknown'}[/dim]")

            max_sections = sections or act.section_count or 100
            count = 0
            with console.status(f"Downloading sections (max {max_sections})..."):
                for i in range(1, max_sections + 1):
                    try:
                        section_cite = UKCitation(
                            type=parsed.type,
                            year=parsed.year,
                            number=parsed.number,
                            section=str(i),
                        )
                        await fetcher.fetch_section(section_cite)
                        count += 1
                        if count % 50 == 0:
                            console.print(f"  [dim]Downloaded {count} sections...[/dim]")
                    except Exception:
                        continue
            return count

        count = asyncio.run(fetch_all())
        console.print(f"[green]Downloaded {count} sections[/green]")


@main.command("extract-guidance")
@click.argument("doc_id")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("data/guidance/extracted"),
    help="Output directory for extracted files",
)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def extract_guidance(doc_id: str, output: Path, as_json: bool):
    """Extract text and parameters from an IRS guidance document.

    Fetches the PDF from IRS.gov, extracts text using PyMuPDF, and parses
    the document structure including inflation-adjusted parameters.

    DOC_ID can be:
      - "2024-40" (assumes Rev. Proc.)
      - "rp-2024-40" (explicit Rev. Proc.)
      - "rr-2024-15" (Revenue Ruling)
      - "notice-2024-78" (Notice)

    Examples:
        axiom extract-guidance 2024-40
        axiom extract-guidance rp-2024-40 --json
        axiom extract-guidance notice-2024-78 -o ./extracted
    """
    from axiom.fetchers.irs_bulk import IRSBulkFetcher, IRSDropDocument
    from axiom.models_guidance import GuidanceType

    # Parse document ID
    doc_id_lower = doc_id.lower()
    if doc_id_lower.startswith("rp-"):
        doc_type = GuidanceType.REV_PROC
        doc_number = doc_id[3:]
    elif doc_id_lower.startswith("rr-"):
        doc_type = GuidanceType.REV_RUL
        doc_number = doc_id[3:]
    elif doc_id_lower.startswith("notice-"):
        doc_type = GuidanceType.NOTICE
        doc_number = doc_id[7:]
    elif doc_id_lower.startswith("n-"):
        doc_type = GuidanceType.NOTICE
        doc_number = doc_id[2:]
    else:
        # Default to Rev. Proc.
        doc_type = GuidanceType.REV_PROC
        doc_number = doc_id

    # Extract year from doc_number
    try:
        year = int(doc_number.split("-")[0])
    except (ValueError, IndexError) as err:
        console.print(f"[red]Invalid document number:[/red] {doc_number}")
        console.print("[dim]Expected format: YYYY-NN (e.g., 2024-40)[/dim]")
        raise SystemExit(1) from err

    # Build filename
    prefix = {
        GuidanceType.REV_PROC: "rp",
        GuidanceType.REV_RUL: "rr",
        GuidanceType.NOTICE: "n",
        GuidanceType.ANNOUNCEMENT: "a",
    }
    year_short = str(year)[2:]  # 2024 -> 24
    num = doc_number.split("-")[1]
    filename = f"{prefix[doc_type]}-{year_short}-{num}.pdf"

    doc = IRSDropDocument(
        doc_type=doc_type,
        doc_number=doc_number,
        year=year,
        pdf_filename=filename,
    )

    console.print(f"[blue]Extracting:[/blue] {doc_type.value} {doc_number}")
    console.print(f"[dim]PDF URL: {doc.pdf_url}[/dim]")

    output.mkdir(parents=True, exist_ok=True)
    pdf_path = output / filename

    with IRSBulkFetcher() as fetcher, console.status("Fetching and extracting..."):
        rev_proc = fetcher.fetch_and_extract(doc, save_pdf=pdf_path)

    console.print(f"[green]Extracted {len(rev_proc.full_text):,} characters[/green]")

    if as_json:
        # Output full result as JSON
        console.print_json(rev_proc.model_dump_json())
    else:
        # Show summary
        console.print()
        console.print(
            Panel(
                f"[bold]{rev_proc.title}[/bold]\n"
                f"[dim]Tax Years: {rev_proc.tax_years}[/dim]\n"
                f"[dim]Effective: {rev_proc.effective_date}[/dim]\n\n"
                f"[bold blue]Sections ({len(rev_proc.sections)}):[/bold blue]\n"
                + "\n".join(
                    f"  {s.section_num}. {s.heading or '(no heading)'}"
                    for s in rev_proc.sections[:10]
                )
                + (
                    f"\n  ... and {len(rev_proc.sections) - 10} more"
                    if len(rev_proc.sections) > 10
                    else ""
                )
                + "\n\n"
                + f"[bold blue]Parameters ({len(rev_proc.parameters)}):[/bold blue]\n"
                + (
                    "\n".join(
                        f"  - {k}: {len(v) if isinstance(v, dict) else v} values"
                        for k, v in rev_proc.parameters.items()
                    )
                    if rev_proc.parameters
                    else "  (none extracted)"
                ),
                title=f"{doc_type.value} {doc_number}",
            )
        )

        # Show extracted parameters in a table if available
        if rev_proc.parameters:
            console.print()
            for param_type, values in rev_proc.parameters.items():
                if isinstance(values, dict) and "max_credit" in values:
                    # EITC-style parameters
                    table = Table(title=f"EITC Parameters ({param_type})")
                    table.add_column("Children", style="cyan")
                    table.add_column("Max Credit", justify="right", style="green")
                    table.add_column("Earned Income Amt", justify="right")
                    table.add_column("Phaseout Start (Joint)", justify="right")
                    table.add_column("Phaseout End (Joint)", justify="right")

                    for n_kids in ["0", "1", "2", "3"]:
                        table.add_row(
                            n_kids,
                            f"${values.get('max_credit', {}).get(n_kids, 'N/A'):,}"
                            if values.get("max_credit", {}).get(n_kids)
                            else "N/A",
                            f"${values.get('earned_income_amount', {}).get(n_kids, 'N/A'):,}"
                            if values.get("earned_income_amount", {}).get(n_kids)
                            else "N/A",
                            f"${values.get('phaseout_start', {}).get('joint', {}).get(n_kids, 'N/A'):,}"
                            if values.get("phaseout_start", {}).get("joint", {}).get(n_kids)
                            else "N/A",
                            f"${values.get('phaseout_end', {}).get('joint', {}).get(n_kids, 'N/A'):,}"
                            if values.get("phaseout_end", {}).get("joint", {}).get(n_kids)
                            else "N/A",
                        )
                    console.print(table)

                elif isinstance(values, dict) and "joint" in values:
                    # Standard deduction-style parameters
                    table = Table(title=f"Standard Deduction ({param_type})")
                    table.add_column("Filing Status", style="cyan")
                    table.add_column("Amount", justify="right", style="green")

                    for status, amount in values.items():
                        if isinstance(amount, (int, float)):
                            table.add_row(status, f"${amount:,}")

                    console.print(table)

        # Save extracted text
        text_path = output / f"{doc_number}.txt"
        text_path.write_text(rev_proc.full_text)
        console.print(f"\n[dim]Text saved to: {text_path}[/dim]")
        console.print(f"[dim]PDF saved to: {pdf_path}[/dim]")


@main.command("get-uk")
@click.argument("citation")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def get_uk(citation: str, as_json: bool):
    """Get UK legislation by citation.

    Fetches from legislation.gov.uk and displays.

    Examples:
        axiom-corpus get-uk "ukpga/2003/1/section/62"
        axiom-corpus get-uk "ITEPA 2003 s.62"
        axiom-corpus get-uk "ukpga/2007/3/section/1" --json
    """
    import asyncio

    from axiom.fetchers.legislation_uk import UKLegislationFetcher
    from axiom.models_uk import UKCitation

    try:
        parsed = UKCitation.from_string(citation)
    except ValueError as e:
        console.print(f"[red]Invalid citation:[/red] {e}")
        raise SystemExit(1) from e

    if not parsed.section:
        console.print(f"[red]Section required:[/red] {citation}")
        console.print("[dim]Use format: ukpga/2003/1/section/62[/dim]")
        raise SystemExit(1)

    fetcher = UKLegislationFetcher()

    with console.status("Fetching..."):
        section = asyncio.run(fetcher.fetch_section(parsed))

    if as_json:
        console.print_json(section.model_dump_json())
    else:
        text_preview = section.text[:2000]
        if len(section.text) > 2000:
            text_preview += "..."

        extent_str = ", ".join(section.extent) if section.extent else "UK-wide"

        console.print(
            Panel(
                f"[bold]{section.citation.short_cite}[/bold]\n"
                f"[dim]Extent: {extent_str}[/dim]\n\n"
                f"[bold blue]{section.title}[/bold blue]\n\n"
                f"{text_preview}\n\n"
                f"[dim]Enacted: {section.enacted_date}[/dim]\n"
                f"[dim]Source: {section.source_url}[/dim]",
                title=section.citation.short_cite,
            )
        )


@main.command("sb")
@click.argument("citation_path")
@click.option("--jurisdiction", "-j", default="us", help="Jurisdiction (us, uk, canada)")
@click.option("--children", "-c", is_flag=True, help="Include direct child sections")
@click.option("--deep", "-d", is_flag=True, help="Include ALL descendants (for encoding)")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def sb(citation_path: str, jurisdiction: str, children: bool, deep: bool, as_json: bool):
    """Query statute from Supabase.

    CITATION_PATH is the canonical path or shorthand like "us/statute/26/32" or "26/32".

    Examples:
        axiom sb 26/32                    # Get 26 USC § 32 (EITC)
        axiom sb 26/24 -c                 # With direct children
        axiom sb 26/32 --deep             # Full text with ALL subsections (for encoding)
        axiom sb "ita/2007/1" -j uk       # UK ITA 2007
        axiom sb 26/32 --json             # Output as JSON
    """
    import json as json_module

    from axiom.query import SupabaseQuery

    query = SupabaseQuery()

    # Deep mode - get full concatenated text for encoding
    if deep:
        text = query.get_section_deep(citation_path, jurisdiction)
        if not text:
            console.print(f"[red]Not found:[/red] {citation_path} ({jurisdiction})")
            raise SystemExit(1)

        if as_json:
            data = {"citation_path": citation_path, "jurisdiction": jurisdiction, "text": text}
            console.print_json(json_module.dumps(data, indent=2))
        else:
            console.print(text)
        return

    if children:
        section = query.get_section_with_children(citation_path, jurisdiction)
        if not section:
            console.print(f"[red]Not found:[/red] {citation_path} ({jurisdiction})")
            raise SystemExit(1)

        if as_json:
            data = {
                "rule": {
                    "id": section.rule.id,
                    "heading": section.rule.heading,
                    "body": section.rule.body,
                    "citation_path": section.rule.citation_path,
                },
                "children": [
                    {"id": c.id, "heading": c.heading, "body": c.body} for c in section.children
                ],
            }
            console.print_json(json_module.dumps(data, indent=2))
        else:
            console.print(
                Panel(
                    f"[bold]{section.rule.citation_path or section.rule.source_path}[/bold]\n"
                    f"[dim]Jurisdiction: {section.rule.jurisdiction}[/dim]\n\n"
                    f"[bold blue]{section.rule.heading or 'Untitled'}[/bold blue]\n\n"
                    f"{(section.rule.body or '')[:1000]}{'...' if section.rule.body and len(section.rule.body) > 1000 else ''}\n\n"
                    f"[dim]Children: {len(section.children)} subsections[/dim]",
                    title=citation_path,
                )
            )
            if section.children:
                table = Table(title="Subsections")
                table.add_column("ID", style="cyan")
                table.add_column("Heading", style="green")
                table.add_column("Body Preview")

                for child in section.children[:10]:
                    body_preview = (child.body or "")[:60]
                    if child.body and len(child.body) > 60:
                        body_preview += "..."
                    table.add_row(
                        child.id[:8] + "...",
                        child.heading or "(no heading)",
                        body_preview,
                    )

                if len(section.children) > 10:
                    table.add_row("...", f"... and {len(section.children) - 10} more", "")

                console.print(table)
    else:
        rule = query.get_section(citation_path, jurisdiction)
        if not rule:
            console.print(f"[red]Not found:[/red] {citation_path} ({jurisdiction})")
            raise SystemExit(1)

        if as_json:
            data = {
                "id": rule.id,
                "heading": rule.heading,
                "body": rule.body,
                "citation_path": rule.citation_path,
                "jurisdiction": rule.jurisdiction,
            }
            console.print_json(json_module.dumps(data, indent=2))
        else:
            console.print(
                Panel(
                    f"[bold]{rule.citation_path or rule.source_path}[/bold]\n"
                    f"[dim]Jurisdiction: {rule.jurisdiction}[/dim]\n\n"
                    f"[bold blue]{rule.heading or 'Untitled'}[/bold blue]\n\n"
                    f"{(rule.body or '')[:2000]}{'...' if rule.body and len(rule.body) > 2000 else ''}",
                    title=citation_path,
                )
            )


@main.command("sb-search")
@click.argument("query")
@click.option("--jurisdiction", "-j", help="Filter by jurisdiction (us, uk, canada)")
@click.option("--limit", "-n", default=10, help="Maximum results")
def sb_search(query: str, jurisdiction: str | None, limit: int):
    """Search the Supabase source corpus.

    Examples:
        axiom sb-search "earned income"
        axiom sb-search "criminal code" -j canada
        axiom sb-search "employment" -j uk -n 20
    """
    from axiom.query import SupabaseQuery

    q = SupabaseQuery()
    results = q.search(query, jurisdiction=jurisdiction, limit=limit)

    if not results:
        console.print(f"[yellow]No results for:[/yellow] {query}")
        return

    table = Table(title=f"Search: {query}")
    table.add_column("Jurisdiction", style="cyan")
    table.add_column("Path", style="green")
    table.add_column("Heading")

    for r in results:
        heading = r.heading or "(no heading)"
        if len(heading) > 40:
            heading = heading[:40] + "..."
        table.add_row(r.jurisdiction.upper(), r.citation_path or r.source_path or r.id[:16], heading)

    console.print(table)


@main.command("sb-stats")
def sb_stats():
    """Show Supabase source corpus statistics."""
    from axiom.query import SupabaseQuery

    query = SupabaseQuery()

    with console.status("Fetching stats from Supabase..."):
        stats = query.get_stats()

    console.print()
    console.print(
        Panel(
            f"[bold green]Total Provisions:[/bold green] {stats['total']:,}\n\n"
            f"[bold cyan]US:[/bold cyan] {stats.get('us', 0):,}\n"
            f"[bold blue]UK:[/bold blue] {stats.get('uk', 0):,}\n"
            f"[bold yellow]Canada:[/bold yellow] {stats.get('canada', 0):,}",
            title="Supabase Corpus Statistics",
        )
    )


if __name__ == "__main__":
    main()
