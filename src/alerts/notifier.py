import asyncio
import logging
import os
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from rich import box

log = logging.getLogger(__name__)
console = Console()

def alert_terminal(opp: dict, sizing: dict, dry_run: bool = True):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    label = "[DRY RUN] " if dry_run else ""
    t = Table(
        box=box.ROUNDED,
        title=f"[bold {'yellow' if dry_run else 'green'}]{label}ARB OPPORTUNITY  {ts}[/bold {'yellow' if dry_run else 'green'}]",
        show_header=True,
        header_style="bold cyan",
    )
    t.add_column("Field", style="cyan", no_wrap=True)
    t.add_column("Value")

    t.add_row("Mode", "[yellow]DRY RUN — no orders placed[/yellow]" if dry_run else "[green]LIVE[/green]")
    t.add_row("Edge (gross %)", f"{opp['profit_pct']*100:.2f}%")
    t.add_row("Implied sum", str(opp["implied_sum"]))
    t.add_row("Total stake", f"[yellow]${sizing['bet_size']}[/yellow]")
    t.add_row("Contracts", f"{sizing['contracts']}")
    t.add_row("Guaranteed payout", f"${sizing['guaranteed_payout']}")
    t.add_row("Gross profit", f"${sizing['gross_profit']}")
    fees = sizing.get("fees", {})
    t.add_row("Fees (worst case)", f"[red]-${fees.get('worst_case_total', 0)}[/red]")
    net_color = "green" if sizing["net_profit"] > 0 else "red"
    t.add_row("Net profit", f"[bold {net_color}]${sizing['net_profit']} ({sizing['net_profit_pct']*100:.2f}%)[/bold {net_color}]")
    t.add_row("Limiting rule", sizing.get("limiting_rule", "—"))
    t.add_row("", "")
    t.add_row("[bold]BUY YES[/bold]", opp["buy_yes"]["platform"].upper())
    t.add_row("  Question", opp["buy_yes"]["question"][:70])
    t.add_row("  YES price", str(opp["buy_yes"]["yes_price"]))
    t.add_row("  Leg size", f"${sizing['leg_yes']['usd']} ({sizing['leg_yes']['contracts']} contracts)")
    t.add_row("  URL", opp["buy_yes"]["url"])
    t.add_row("", "")
    t.add_row("[bold]BUY NO[/bold]", opp["buy_no"]["platform"].upper())
    t.add_row("  Question", opp["buy_no"]["question"][:70])
    t.add_row("  NO price", str(opp["buy_no"]["no_price"]))
    t.add_row("  Leg size", f"${sizing['leg_no']['usd']} ({sizing['leg_no']['contracts']} contracts)")
    t.add_row("  URL", opp["buy_no"]["url"])

    console.print(t)

async def alert_sms(opp: dict, sizing: dict, to: str, from_: str, dry_run: bool = True):
    if not to or not from_:
        return
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    if not account_sid or not auth_token:
        log.warning("Twilio credentials missing — SMS skipped")
        return

    prefix = "[DRY RUN] " if dry_run else ""
    body = (
        f"{prefix}ARB {sizing['net_profit_pct']*100:.1f}% net — ${sizing['bet_size']} stake → ${sizing['net_profit']} after fees\n"
        f"YES ({opp['buy_yes']['platform']}): ${sizing['leg_yes']['usd']}\n"
        f"NO  ({opp['buy_no']['platform']}): ${sizing['leg_no']['usd']}"
    )

    try:
        from twilio.rest import Client
        client = Client(account_sid, auth_token)
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: client.messages.create(body=body, from_=from_, to=to),
        )
        log.info("SMS sent to %s", to)
    except Exception as e:
        log.warning("SMS alert failed: %s", e)
