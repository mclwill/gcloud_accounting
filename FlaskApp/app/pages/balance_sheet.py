import requests
import dash
from dash import html, dcc, Input, Output, State, callback, no_update
from datetime import date, datetime
from urllib.parse import parse_qs, urlencode
from flask import session as flask_session

import FlaskApp.app.common as common

dash.register_page(__name__, path="/balance-sheet")


def API_TOKEN() -> str:
    return common.access_secret_version("global_parameters", None, "api_token")


AUD_ACCOUNTING_FMT = "_($* #,##0.00_);_($* (#,##0.00);_($* \"-\"??_);_(@_)"

FISCAL_YEAR_START_MONTH = 7  # July
FISCAL_YEAR_END_MONTH = 6    # June
FISCAL_YEAR_END_DAY = 30     # 30th


def _parse_asof_from_url(search: str | None) -> date | None:
    qs = parse_qs((search or "").lstrip("?"))
    s = qs.get("asof", [None])[0]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def _fy_start_year_for(d: date) -> int:
    # FY starts July 1. If month >= July => FY start is current year else previous year.
    return d.year if d.month >= FISCAL_YEAR_START_MONTH else d.year - 1


def _previous_fy_end(d: date) -> date:
    # Previous FY end is June 30 of FY start year.
    fy_start_year = _fy_start_year_for(d)
    return date(fy_start_year, FISCAL_YEAR_END_MONTH, FISCAL_YEAR_END_DAY)


def _fy_label_for(d: date) -> str:
    # Label like "Jul. 2024 - Jun. 2025"
    fy_start_year = _fy_start_year_for(d)
    fy_end_year = fy_start_year + 1
    return f"Jul. {fy_start_year} - Jun. {fy_end_year}"


def _fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""


def render_bs_preview(bs: dict) -> html.Div:
    cols = bs.get("as_of", [])
    col_labels = [c.get("label", "") for c in cols]
    n = len(col_labels)

    def header_row():
        return html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["160px"] * n),
                "gap": "8px",
                "fontWeight": "600",
                "padding": "8px 6px",
                "borderBottom": "1px solid #ddd",
            },
            children=[
                html.Div("Account"),
                *[html.Div(lbl, style={"textAlign": "right"}) for lbl in col_labels],
            ],
        )

    blocks = [header_row()]

    for sec in bs.get("sections", []):
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
            vec = row.get("cols", [0.0] * n)

            is_total = kind == "total"
            is_group = kind == "group"

            blocks.append(
                html.Div(
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["160px"] * n),
                        "gap": "8px",
                        "padding": "6px 6px",
                        "borderBottom": "1px solid #f0f0f0",
                        "fontWeight": "700" if (is_total or is_group) else "400",
                    },
                    children=[
                        html.Div(label, style={"paddingLeft": f"{level * 16}px"}),
                        *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in vec],
                    ],
                )
            )

        # Section total (vertical only)
        sec_totals = sec.get("section_totals", [0.0] * n)
        blocks.append(
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["160px"] * n),
                    "gap": "8px",
                    "padding": "8px 6px",
                    "borderTop": "1px solid #ddd",
                    "fontWeight": "800",
                },
                children=[
                    html.Div(f"Total {sec_name}"),
                    *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in sec_totals],
                ],
            )
        )

    # Accounting equation check
    diff = bs.get("totals", {}).get("difference", [])
    if diff:
        blocks.append(html.Div(style={"height": "10px"}))
        blocks.append(
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "minmax(260px, 1fr) " + " ".join(["160px"] * n),
                    "gap": "8px",
                    "padding": "10px 6px",
                    "fontWeight": "800",
                },
                children=[
                    html.Div("Assets - (Liabilities + Equity)"),
                    *[html.Div(_fmt_money(v), style={"textAlign": "right"}) for v in diff],
                ],
            )
        )

    return html.Div(
        blocks,
        style={
            "maxWidth": "1100px",
            "border": "1px solid #eee",
            "borderRadius": "10px",
            "padding": "10px",
        },
    )



layout = html.Div(
    style={"padding": "16px"},
    children=[
        dcc.Location(id="bs-url"),
        #html.H3("Balance Sheet"),

        html.Div(id="bs-range-label", style={'visibility':'hidden'}),

        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"},
            children=[
                dcc.DatePickerSingle(
                    id="bs-asof",
                    date=date.today(),
                    display_format="YYYY-MM-DD",
                ),
                dcc.Checklist(
                    id="bs-compare-prevfy",
                    options=[{"label": "Compare previous FY end", "value": "on"}],
                    value=["on"],
                    style={"marginLeft": "10px"},
                ),
                html.Button("Export Excel", id="bs-export-btn"),
                dcc.Download(id="bs-download"),
            ],
        ),

        dcc.Loading(
            id="bs-loading",
            type="default",
            children=html.Div(id="bs-preview", style={"marginTop": "16px"}),
        ),

        html.Div(id="bs-status", style={"marginTop": "8px"}),
    ],
)


@callback(
    Output("bs-asof", "date"),
    Output("bs-range-label", "children"),
    Input("bs-url", "search"),
)
def sync_asof_from_url(search):
    d = _parse_asof_from_url(search)
    if not d:
        d = date.today()
    return d, f"As of: {d.isoformat()}"


def _build_query(asof: date, compare_prevfy: bool) -> tuple[str, str]:
    """
    Returns (json_url, xlsx_url) including query params.
    c1 = previous FY end (Jun 30 of FY start year)
    c2 = as-of date, labelled as FY label (Jul YYYY - Jun YYYY)
    """

    base_json = common.absolute_url(f"/api/reports/balance_sheet")
    base_xlsx = common.absolute_url(f"/api/reports/balance_sheet.xlsx")

    if compare_prevfy:
        prev_end = _previous_fy_end(asof)
        params = {
            "c1_label": f"30 Jun., {prev_end.year}",
            "c1_date": prev_end.isoformat(),
            "c2_label": asof.strftime("%d %b %Y"),  
            "c2_date": asof.isoformat(),
        }
    else:
        params = {
            "c1_label": f"As of {asof.isoformat()}",
            "c1_date": asof.isoformat(),
        }

    # Ensure the API runs for the currently selected entity.
    entity_name = flask_session.get("current_entity")
    if entity_name:
        params["entity"] = entity_name

    qs = urlencode(params)
    return f"{base_json}?{qs}", f"{base_xlsx}?{qs}"


@callback(
    Output("bs-preview", "children"),
    Output("bs-status", "children", allow_duplicate=True),
    Output("bs-range-label", "children", allow_duplicate=True),
    Input("bs-asof", "date"),
    Input("bs-compare-prevfy", "value"),
    prevent_initial_call=True,
)
def load_bs_preview(asof_date, compare_vals):
    if not asof_date:
        return html.Div("Choose an as-of date to preview."), "",no_update

    asof = date.fromisoformat(asof_date)
    compare_prevfy = "on" in (compare_vals or [])

    json_url, _ = _build_query(asof, compare_prevfy)

    try:
        resp = requests.get(
            json_url,
            headers={"X-Internal-Token": API_TOKEN()},
            verify=common.api_verify,
            timeout=30,
        )
    except Exception as e:
        return html.Div(), f"Preview failed: {e}", no_update

    if resp.status_code != 200:
        msg = f"Preview failed ({resp.status_code})"
        try:
            msg = resp.json().get("error", msg)
        except Exception:
            pass
        return html.Div(), msg, no_update

    bs = resp.json()
    return render_bs_preview(bs), "", asof_date


@callback(
    Output("bs-download", "data"),
    Output("bs-status", "children"),
    Input("bs-export-btn", "n_clicks"),
    State("bs-asof", "date"),
    State("bs-compare-prevfy", "value"),
    prevent_initial_call=True,
)
def export_bs(_, asof_date, compare_vals):
    if not asof_date:
        return no_update, "Please choose an as-of date."

    asof = date.fromisoformat(asof_date)
    compare_prevfy = "on" in (compare_vals or [])

    _, xlsx_url = _build_query(asof, compare_prevfy)

    try:
        resp = requests.get(
            xlsx_url,
            headers={"X-Internal-Token": API_TOKEN()},
            verify=False,
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

    filename = f"balance_sheet_{asof.isoformat()}.xlsx"
    return dcc.send_bytes(resp.content, filename), "Exported."
