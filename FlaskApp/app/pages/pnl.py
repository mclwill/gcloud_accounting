import requests
import dash
from dash import html, dcc, Input, Output, State, callback, no_update
from datetime import date, timedelta
from urllib.parse import parse_qs, urlencode

import FlaskApp.app.common as common

dash.register_page(__name__, path="/pnl")

def API_TOKEN() -> str:
    return common.access_secret_version("global_parameters", None, "api_token")


FISCAL_YEAR_START_MONTH = 7  # July
FISCAL_YEAR_START_DAY = 1


def fy_start_for(d: date) -> date:
    fy_year = d.year if d.month >= FISCAL_YEAR_START_MONTH else d.year - 1
    return date(fy_year, FISCAL_YEAR_START_MONTH, FISCAL_YEAR_START_DAY)

def prev_fy_range_for(end: date) -> tuple[date, date]:
    cur_start = fy_start_for(end)
    prev_start = date(cur_start.year - 1, FISCAL_YEAR_START_MONTH, FISCAL_YEAR_START_DAY)
    prev_end = cur_start - timedelta(days=1)  # 30 Jun
    return prev_start, prev_end

layout = html.Div(
    style={"padding": "16px"},
    children=[
        dcc.Location(id="pnl-url"),
        #html.H3("Profit & Loss"),

        html.Div(id="pnl-range-label", style={"marginBottom": "10px"}),

        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center"},
            children=[
                dcc.DatePickerRange(
                    id="pnl-range",
                    start_date=date.today().replace(day=1),
                    end_date=date.today(),
                    display_format="YYYY-MM-DD",
                ),
                html.Button("Export Excel", id="pnl-export-btn"),
                dcc.Download(id="pnl-download"),
            ],
        ),

        dcc.Loading(
            id="pnl-loading",
            type="default",
            children=html.Div(id="pnl-preview", style={"marginTop": "16px"}),
        ),

        html.Div(id="pnl-status", style={"marginTop": "8px"}),

    ],
)

def _fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""
    

def render_pnl_preview(pnl: dict) -> html.Div:
    periods = pnl.get("periods", [])
    period_labels = [p.get("label", "") for p in periods]
    n = len(period_labels)

    def header_row():
        return html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["140px"] * n) + " 140px",
                "gap": "8px",
                "fontWeight": "600",
                "padding": "8px 6px",
                "borderBottom": "1px solid #ddd",
            },
            children=[
                html.Div("Account"),
                *[html.Div(lbl, style={"textAlign": "right"}) for lbl in period_labels],
                html.Div("Total", style={"textAlign": "right"}),
            ],
        )

    blocks = [header_row()]

    for sec in pnl.get("sections", []):
        sec_name = sec.get("section", "")
        blocks.append(
            html.Div(
                sec_name,
                style={"fontWeight": "700", "marginTop": "14px", "padding": "6px 2px"},
            )
        )

        for row in sec.get("rows", []):
            label = row.get("label", "")
            level = int(row.get("level", 0))
            kind = row.get("kind", "account")
            vec = row.get("periods", [0.0] * n)
            row_total = row.get("row_total", sum(vec))

            is_total = kind == "total"
            is_group = kind == "group"

            blocks.append(
                html.Div(
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["140px"] * n) + " 140px",
                        "gap": "8px",
                        "padding": "6px 6px",
                        "borderBottom": "1px solid #f0f0f0",
                        "fontWeight": "700" if (is_total or is_group) else "400",
                    },
                    children=[
                        html.Div(
                            label,
                            style={"paddingLeft": f"{level * 16}px"},
                        ),
                        *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in vec],
                        html.Div(_fmt_money(row_total), style={"textAlign": "right"}),
                    ],
                )
            )

        # Total section line
        sec_totals = sec.get("section_totals", [0.0] * n)
        sec_total = sec.get("section_total", sum(sec_totals))
        blocks.append(
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["140px"] * n) + " 140px",
                    "gap": "8px",
                    "padding": "8px 6px",
                    "borderTop": "1px solid #ddd",
                    "fontWeight": "700",
                },
                children=[
                    html.Div(f"Total {sec_name}"),
                    *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in sec_totals],
                    html.Div(_fmt_money(sec_total), style={"textAlign": "right"}),
                ],
            )
        )

    # Summary lines (gross + net)
    totals = pnl.get("totals", {})
    gp = totals.get("gross_profit", [0.0] * n)
    ne = totals.get("net_profit", [0.0] * n)

    def summary_line(label, vec):
        return html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["140px"] * n) + " 140px",
                "gap": "8px",
                "padding": "10px 6px",
                "fontWeight": "800",
            },
            children=[
                html.Div(label),
                *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in vec],
                html.Div(_fmt_money(sum(vec)), style={"textAlign": "right"}),
            ],
        )

    blocks.append(html.Div(style={"height": "10px"}))
    blocks.append(summary_line("Gross Profit", gp))
    blocks.append(summary_line("Net Earnings", ne))

    return html.Div(
        blocks,
        style={
            "maxWidth": "1100px",
            "border": "1px solid #eee",
            "borderRadius": "10px",
            "padding": "10px",
        },
    )


@callback(
    Output("pnl-range", "start_date"),
    Output("pnl-range", "end_date"),
    Output("pnl-range-label", "children"),
    Input("pnl-url", "search"),
)
def sync_range_from_url(search):
    qs = parse_qs((search or "").lstrip("?"))

    start = qs.get("start", [None])[0]
    end = qs.get("end", [None])[0]

    today = date.today()

    if start and end:
        label = f"Reporting period: {start} → {end}"
        return start, end, label

    # ✅ Default to current FY
    fy_start = fy_start_for(today)
    label = f"Reporting period: {fy_start.isoformat()} → {today.isoformat()}"
    return fy_start, today, label

@callback(
    Output("pnl-download", "data"),
    Output("pnl-status", "children"),
    Input("pnl-export-btn", "n_clicks"),
    State("pnl-range", "start_date"),
    State("pnl-range", "end_date"),
    prevent_initial_call=True,
)
def export_pnl(_, start_date, end_date):
    if not start_date or not end_date:
        return no_update, "Please choose a start and end date."

    # current FY-to-date based on chosen end_date
    end = date.fromisoformat(end_date)
    cur_start = fy_start_for(end)
    prev_start, prev_end = prev_fy_range_for(end)

    params = {
        # Previous FY (left column)
        "p1_label": f"{prev_start.strftime('%d %b %Y')} - {prev_end.strftime('%d %b %Y')}",
        "p1_start": prev_start.isoformat(),
        "p1_end": prev_end.isoformat(),

        # Current FY-to-date (right column)
        "p2_label": f"{cur_start.strftime('%d %b %Y')} - {end.strftime('%d %b %Y')}",
        "p2_start": cur_start.isoformat(),
        "p2_end": end.isoformat(),
    }

    url = common.absolute_url(f"/api/reports/pnl?" + urlencode(params))

    try:
        resp = requests.get(
            url,
            headers={"X-Internal-Token": API_TOKEN()},
            verify=common.api_verify,
            timeout=30,
        )
    except Exception as e:
        return no_update, f"Export failed: {e}"

    if resp.status_code != 200:
        msg = f"Export failed ({resp.status_code})"
        try:
            msg = resp.json().get("error", msg)
        except Exception:
            pass
        return no_update, msg

    filename = f"profit_and_loss_{start_date}_to_{end_date}.xlsx"
    return dcc.send_bytes(resp.content, filename), "Exported."


@callback(
    Output("pnl-preview", "children"),
    Output("pnl-status", "children", allow_duplicate=True),
    Input("pnl-range", "start_date"),
    Input("pnl-range", "end_date"),
    prevent_initial_call=True,
)
def load_pnl_preview(start_date, end_date):
    if not start_date or not end_date:
        return html.Div("Choose a date range to preview."), ""

    # current FY-to-date based on chosen end_date
    end = date.fromisoformat(end_date)
    cur_start = fy_start_for(end)
    prev_start, prev_end = prev_fy_range_for(end)

    params = {
        # Previous FY (left column)
        "p1_label": f"{prev_start.strftime('%d %b %Y')} - {prev_end.strftime('%d %b %Y')}",
        "p1_start": prev_start.isoformat(),
        "p1_end": prev_end.isoformat(),

        # Current FY-to-date (right column)
        "p2_label": f"{cur_start.strftime('%d %b %Y')} - {end.strftime('%d %b %Y')}",
        "p2_start": cur_start.isoformat(),
        "p2_end": end.isoformat(),
    }

    url = common.absolute_url(f'api/reports/pnl?' + urlencode(params))

    try:
        resp = requests.get(
            url,
            headers={"X-Internal-Token": API_TOKEN()},
            verify=common.api_verify,
            timeout=30,
        )
    except Exception as e:
        return html.Div(), f"Preview failed: {e}"

    if resp.status_code != 200:
        msg = f"Preview failed ({resp.status_code})"
        try:
            msg = resp.json().get("error", msg)
        except Exception:
            pass
        return html.Div(), msg

    pnl = resp.json()
    return render_pnl_preview(pnl), ""

