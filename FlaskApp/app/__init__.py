from flask import Flask, session
from flask_login import login_required
from flask_migrate import Migrate
import dash
import dash_bootstrap_components as dbc
import os, stat, pwd, grp
from urllib.parse import urlparse, unquote

from .accounting_db import db  # ← IMPORT db, do not create it here
from .services.entities import get_entities

SESSION_ENTITY_KEY = "current_entity"
DEFAULT_ENTITY = "JAJG Pty Ltd"

def uri_to_path(db_uri: str) -> str:
    # Only convert sqlite URIs; return others unchanged or raise
    if db_uri.startswith("sqlite:"):
        u = urlparse(db_uri)
        # sqlite:////absolute/path.db -> u.path == /absolute/path.db
        return unquote(u.path)
    return db_uri  # for non-sqlite, you might just print the uri

def print_perms(db_uri_or_path: str) -> None:
    path = uri_to_path(db_uri_or_path)

    st = os.stat(path)
    owner = pwd.getpwuid(st.st_uid).pw_name
    group = grp.getgrgid(st.st_gid).gr_name

    print(f"{path}")
    print(f"  perms : {stat.filemode(st.st_mode)} ({oct(st.st_mode & 0o777)})")
    print(f"  owner : {owner}")
    print(f"  group : {group}")

    d = os.path.dirname(path) or "."
    dst = os.stat(d)
    downer = pwd.getpwuid(dst.st_uid).pw_name
    dgroup = grp.getgrgid(dst.st_gid).gr_name

    print(f"{d}/")
    print(f"  perms : {stat.filemode(dst.st_mode)} ({oct(dst.st_mode & 0o777)})")
    print(f"  owner : {downer}")
    print(f"  group : {dgroup}")

app = Flask(__name__)
app.config.from_prefixed_env()

@app.before_request
def ensure_entity():
    if SESSION_ENTITY_KEY not in session:
        session[SESSION_ENTITY_KEY] = DEFAULT_ENTITY

@app.context_processor
def inject_entity():
    return {
        "current_entity": session.get(SESSION_ENTITY_KEY),
        "all_entities": get_entities(),  # reuse your existing service
    }

# Mac version
if ('LOCAL' in app.config) and app.config['LOCAL']:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:////Users/gary/.local/accounting/accounting.db"
#Google Cloud version
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:////var/www/FlaskApp/instance/accounting.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

print(app.config["SQLALCHEMY_DATABASE_URI"])
print_perms(app.config["SQLALCHEMY_DATABASE_URI"])
print_perms(os.path.dirname(app.config["SQLALCHEMY_DATABASE_URI"]))

app.config["API_DEBUG"] = True

# Initialise extensions
db.init_app(app)
migrate = Migrate(app, db)

# Import models so Alembic sees them
from .models import entity, account, transaction, transaction_line, csv_account_mapping  # ← IMPORTANT

from .routes.transactions_api import bp as transactions_api_bp
from .routes.accounts_api import bp as accounts_api_bp
from .ui.transactions_ui import bp as transactions_ui_bp
from .ui.accounts_ui import bp as accounts_ui_bp
from .ui.app_ui import bp as app_ui_bp
from .routes.reports_api import bp as reports_api_bp
from .ui.reports_ui import bp as reports_ui_bp

app.register_blueprint(transactions_api_bp, url_prefix="/api")
app.register_blueprint(accounts_api_bp, url_prefix="/api")
app.register_blueprint(transactions_ui_bp)
app.register_blueprint(accounts_ui_bp)
app.register_blueprint(app_ui_bp)
app.register_blueprint(reports_api_bp, url_prefix="/api/reports")
app.register_blueprint(reports_ui_bp)

# Auth & views
from . import auth_real_python
from . import views

# Dash setup
external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
]

external_scripts = [
    "https://unpkg.com/@popperjs/core@2/dist/umd/popper.min.js",
    "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js",
]

dash_app = dash.Dash(
    __name__,
    server=app,
    url_base_pathname="/dash/",
    use_pages=True,
    pages_folder=os.path.join(os.path.dirname(__file__), "pages"),
    external_stylesheets=external_stylesheets,
    external_scripts=external_scripts,
    suppress_callback_exceptions = True
)

from .pages import ledger
from .pages import pnl
from .pages import balance_sheet
from .pages import accounts



# Protect Dash routes
'''for view_func in app.view_functions:
    if view_func.startswith(dash_app.config["routes_pathname_prefix"]):
        app.view_functions[view_func] = login_required(app.view_functions[view_func])
#moved to below code in auth_real_python.py
@app.before_request
def protect_dash():
    if request.path.startswith("/dash"):
        if not current_user.is_authenticated:
            return redirect(url_for("login"))
'''

if __name__ == "__main__":
    dash_main.dash_app.run_server(debug=True)
