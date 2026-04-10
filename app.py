import io
import csv
import tempfile
from urllib.parse import urlparse
from flask import send_file
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from pypdf import PdfReader, PdfWriter
from PIL import Image
from werkzeug.security import generate_password_hash, check_password_hash
import re
import os
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, flash
from supabase import create_client, Client

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)

FLASK_SECRET_KEY = (os.getenv("FLASK_SECRET_KEY") or "katramoney_fixed_2026").strip().strip('"')

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"))
app.secret_key = FLASK_SECRET_KEY

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().strip('"').rstrip("/")
SUPABASE_ANON_KEY = (os.getenv("SUPABASE_ANON_KEY") or "").strip().strip('"')
SUPABASE_SERVICE_KEY = (
    os.getenv("SUPABASE_SERVICE_KEY")
    or os.getenv("SUPABASE_SERVICE_ROLE")
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or ""
).strip().strip('"')

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is missing")
if not SUPABASE_ANON_KEY:
    raise RuntimeError("SUPABASE_ANON_KEY is missing")
if not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY / SUPABASE_SERVICE_KEY is missing")

SUPABASE_AUTH_URL = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
supabase_admin: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# =========================================================
# HELPERS
# =========================================================
def to_str(v, default=""):
    if v is None:
        return default
    return str(v).strip()

def to_float(v, default=0):
    try:
        if v is None or str(v).strip() == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)

def to_int(v, default=0):
    try:
        if v is None or str(v).strip() == "":
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)

def to_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in ["1", "true", "yes", "on", "active"]

def rows_of(result):
    try:
        data = getattr(result, "data", None)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def first_of(result):
    rows = rows_of(result)
    return rows[0] if rows else {}

def wants_json():
    try:
        if request.is_json:
            return True
        accept = (request.headers.get("Accept") or "").lower()
        content_type = (request.headers.get("Content-Type") or "").lower()
        xrw = (request.headers.get("X-Requested-With") or "").lower()
        return (
            "application/json" in accept
            or "application/json" in content_type
            or xrw == "xmlhttprequest"
        )
    except Exception:
        return False

def json_ok(message="OK", data=None, **extra):
    payload = {"success": True, "message": message, "data": data or []}
    payload.update(extra)
    return jsonify(payload)

def json_error(message, status=500, **extra):
    payload = {"success": False, "error": str(message)}
    payload.update(extra)
    return jsonify(payload), status

def safe_rows(table_name, order_col=None, desc=False):
    try:
        q = supabase_admin.table(table_name).select("*")
        if order_col:
            q = q.order(order_col, desc=desc)
        return rows_of(q.execute())
    except Exception as e:
        print(f"TABLE READ ERROR [{table_name}]:", e)
        return []

def safe_first(table_name):
    try:
        return first_of(
            supabase_admin.table(table_name).select("*").limit(1).execute()
        )
    except Exception as e:
        print(f"TABLE FIRST ERROR [{table_name}]:", e)
        return {}

def safe_find_by_id(table_name, row_id):
    try:
        return first_of(
            supabase_admin.table(table_name).select("*").eq("id", row_id).limit(1).execute()
        )
    except Exception as e:
        print(f"TABLE FIND ERROR [{table_name}]:", e)
        return {}

def upsert_singleton(table_name, payload):
    existing = safe_first(table_name)
    if existing and existing.get("id"):
        return supabase_admin.table(table_name).update(payload).eq("id", existing["id"]).execute()
    return supabase_admin.table(table_name).insert(payload).execute()

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged_in"):
            if request.path.startswith("/api/") or wants_json():
                return json_error("Unauthorized", 401)
            return redirect(url_for("admin_login"))
        return fn(*args, **kwargs)
    return wrapper

def is_admin_user(user_id=None, email=None):
    try:
        if user_id:
            r = supabase_admin.table("admin_users").select("*").eq("user_id", user_id).limit(1).execute()
            if rows_of(r):
                return True
        if email:
            r = supabase_admin.table("admin_users").select("*").eq("email", email).limit(1).execute()
            if rows_of(r):
                return True
        return False
    except Exception as e:
        print("ADMIN CHECK ERROR:", e)
        return False

def get_applications():
    rows = []
    if rows:
        return "loan_applications", rows
    rows = safe_rows("applications", "created_at", True)
    return "applications", rows

def get_application_by_id(app_id):
    row = {}
    if row:
        return "loan_applications", row
    row = safe_find_by_id("applications", app_id)
    if row:
        return "applications", row
    return None, {}

def extract_documents(application_row):
    docs = []
    keys = [
        "documents", "document_urls", "attachments", "uploaded_documents",
        "bank_statement_url", "payslip_url", "proof_of_income_url",
        "proof_of_residence_url", "id_copy_url", "id_front_url", "id_back_url",
        "passport_url", "selfie_url", "national_id_url"
    ]

    for key in keys:
        val = application_row.get(key)
        if not val:
            continue

        if isinstance(val, list):
            for item in val:
                if item:
                    docs.append({"label": key, "url": item})
        elif isinstance(val, dict):
            for k, v in val.items():
                if v:
                    docs.append({"label": k, "url": v})
        else:
            docs.append({"label": key, "url": val})

    clean = []
    seen = set()
    for d in docs:
        u = to_str(d.get("url"))
        if u and u not in seen:
            seen.add(u)
            clean.append({"label": d.get("label", "document"), "url": u})
    return clean


# =========================================================
# AUTH
# =========================================================
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None

    if request.method == "GET" and request.args.get("fresh") == "1":
        session.clear()

    if session.get("admin_logged_in") and request.method == "GET" and request.args.get("fresh") != "1":
        return redirect(url_for("admin_dashboard"))

    if request.method == "POST":
        email = to_str(request.form.get("username")).lower()
        password = request.form.get("password") or ""

        if not email or not password:
            error = "Enter admin email and password."
        else:
            try:
                headers = {
                    "apikey": SUPABASE_ANON_KEY,
                    "Content-Type": "application/json"
                }
                payload = {
                    "email": email,
                    "password": password
                }
                auth_res = requests.post(SUPABASE_AUTH_URL, headers=headers, json=payload, timeout=20)

                if not auth_res.ok:
                    error = "Invalid admin email or password."
                else:
                    auth_data = auth_res.json()
                    user = auth_data.get("user") or {}
                    user_id = user.get("id")
                    user_email = (user.get("email") or email).lower()

                    if is_admin_user(user_id, user_email):
                        session["admin_logged_in"] = True
                        session["admin_email"] = user_email
                        session["admin_user_id"] = user_id
                        return redirect(url_for("admin_dashboard"))
                    else:
                        error = "User is not registered as admin."
            except Exception as e:
                error = f"Login failed: {e}"

    if os.path.exists(os.path.join(BASE_DIR, "templates", "admin_login.html")):
        return render_template("admin_login.html", error=error)

    return f"""
    <html>
    <body style="font-family:Arial;padding:40px;">
      <h2>KATRAMONEY Admin Login</h2>
      <form method="post">
        <input name="username" placeholder="Admin Email" style="padding:10px;width:320px;display:block;margin-bottom:10px;">
        <input name="password" type="password" placeholder="Password" style="padding:10px;width:320px;display:block;margin-bottom:10px;">
        <button type="submit" style="padding:10px 16px;">Login</button>
      </form>
      <div style="color:red;margin-top:16px;">{error or ""}</div>
    </body>
    </html>
    """

@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


# =========================================================
# PUBLIC
# =========================================================
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/apply")
def apply():
    return render_template("apply.html")

@app.route("/favicon.ico")
def favicon():
    return ("", 204)

@app.route("/api/public-site-data", methods=["GET"])
def api_public_site_data():
    try:
        site_config = safe_first("site_config")
        site_settings = safe_first("site_settings")
        products = safe_rows("loan_products", "id", False)

        clean_products = []
        for p in products:
            if p.get("active") is False:
                continue
            clean_products.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "description": p.get("description") or "",
                "interest_rate": to_float(p.get("interest_rate"), 0),
                "service_fee": to_float(p.get("service_fee"), 0),
                "min_amount": to_int(p.get("min_amount"), 0),
                "max_amount": to_int(p.get("max_amount"), 0),
                "featured": to_bool(p.get("featured"), False),
                "active": to_bool(p.get("active"), True),
                "icon": p.get("icon") or "💳",
                "terms": p.get("terms") if isinstance(p.get("terms"), list) else [],
                "features": p.get("features") if isinstance(p.get("features"), list) else []
            })

        return jsonify({
            "success": True,
            "site_config": site_config,
            "site_settings": site_settings,
            "loan_products": clean_products
        })
    except Exception as e:
        print("PUBLIC DATA ERROR:", e)
        return json_error(e, 500)


# =========================================================
# ADMIN DASHBOARD
# =========================================================
@app.route("/admin")
@admin_required
def admin_dashboard():
    try:
        app_table, applications = get_applications()
        loan_products = safe_rows("loan_products", "id", False)
        support_tickets = safe_rows("support_tickets", "created_at", True)
        customer_messages = safe_rows("customer_messages", "created_at", True)
        contacts = safe_rows("contacts", "created_at", True)
        customer_profiles = safe_rows("customer_profiles", "created_at", True)
        blacklist = safe_rows("blacklist", "created_at", True)
        site_config = safe_first("site_config")
        site_settings = safe_first("site_settings")

        pending = len([x for x in applications if str(x.get("status") or "").upper() in ["PENDING", "NEW", "UNDER REVIEW", "UNDER_REVIEW"]])
        approved = len([x for x in applications if str(x.get("status") or "").upper() == "APPROVED"])
        rejected = len([x for x in applications if str(x.get("status") or "").upper() == "REJECTED"])
        blocked = len([x for x in applications if str(x.get("status") or "").upper() == "BLOCKED"])
        total_requested = sum([to_float(x.get("amount"), 0) for x in applications])

        return render_template(
            "admin.html",
            application_table=app_table,
            apps=applications,
            loan_products=loan_products,
            support_tickets=support_tickets,
            customer_messages=customer_messages,
            contacts=contacts,
            customer_profiles=customer_profiles,
            blacklist=blacklist,
            site_config=site_config,
            site_settings=site_settings,
            pending=pending,
            approved=approved,
            rejected=rejected,
            blocked=blocked,
            total_requested=total_requested
        )
    except Exception as e:
        return f"ADMIN DASHBOARD ERROR: {e}", 500


# =========================================================
# ADMIN PRODUCTS
# =========================================================
@app.route("/api/admin/loan-products", methods=["GET"])
@admin_required
def api_admin_loan_products():
    try:
        products = safe_rows("loan_products", "id", False)
        return json_ok("Loan products loaded.", products)
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/products/create", methods=["POST"])
@admin_required
def admin_create_product():
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}

        payload = {
            "name": to_str(body.get("name")),
            "description": to_str(body.get("description")),
            "interest_rate": to_float(body.get("interest_rate"), 0),
            "service_fee": to_float(body.get("service_fee"), 0),
            "min_amount": to_int(body.get("min_amount"), 0),
            "max_amount": to_int(body.get("max_amount"), 0),
            "featured": to_bool(body.get("featured"), False),
            "active": to_bool(body.get("active"), True),
            "icon": to_str(body.get("icon"), "💳")
        }

        if not payload["name"]:
            return json_error("Product name is required.", 400)

        if payload["max_amount"] and payload["min_amount"] and payload["max_amount"] < payload["min_amount"]:
            return json_error("Max amount cannot be less than min amount.", 400)

        result = supabase_admin.table("loan_products").insert(payload).execute()

        if wants_json():
            return json_ok("Loan product created successfully.", rows_of(result))

        flash("Loan product created successfully.", "success")
        return redirect(url_for("admin_dashboard"))
    except Exception as e:
        print("CREATE PRODUCT ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to create product: {e}", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/products/<product_id>/update", methods=["POST"])
@admin_required
def admin_update_product(product_id):
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}

        payload = {
            "name": to_str(body.get("name")),
            "description": to_str(body.get("description")),
            "interest_rate": to_float(body.get("interest_rate"), 0),
            "service_fee": to_float(body.get("service_fee"), 0),
            "min_amount": to_int(body.get("min_amount"), 0),
            "max_amount": to_int(body.get("max_amount"), 0),
            "featured": to_bool(body.get("featured"), False),
            "active": to_bool(body.get("active"), True),
            "icon": to_str(body.get("icon"), "💳")
        }

        if not payload["name"]:
            return json_error("Product name is required.", 400)

        if payload["max_amount"] and payload["min_amount"] and payload["max_amount"] < payload["min_amount"]:
            return json_error("Max amount cannot be less than min amount.", 400)

        result = supabase_admin.table("loan_products").update(payload).eq("id", product_id).execute()

        if wants_json():
            return json_ok("Loan product updated successfully.", rows_of(result))

        flash("Loan product updated successfully.", "success")
        return redirect(url_for("admin_dashboard"))
    except Exception as e:
        print("UPDATE PRODUCT ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to update product: {e}", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/products/<product_id>/delete", methods=["POST"])
@admin_required
def admin_delete_product(product_id):
    try:
        result = supabase_admin.table("loan_products").delete().eq("id", product_id).execute()

        if wants_json():
            return json_ok("Loan product deleted successfully.", rows_of(result))

        flash("Loan product deleted successfully.", "success")
        return redirect(url_for("admin_dashboard"))
    except Exception as e:
        print("DELETE PRODUCT ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to delete product: {e}", "error")
        return redirect(url_for("admin_dashboard"))


# =========================================================
# ADMIN MESSAGES / SUPPORT
# =========================================================
@app.route("/api/admin/messages-feed", methods=["GET"])
@admin_required
def api_admin_messages_feed():
    try:
        contacts = safe_rows("contacts", "created_at", True)
        customer_messages = safe_rows("customer_messages", "created_at", True)
        support_tickets = safe_rows("support_tickets", "created_at", True)

        return jsonify({
            "success": True,
            "contacts": contacts,
            "customer_messages": customer_messages,
            "support_tickets": support_tickets
        })
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/messages/<message_id>/reply", methods=["POST"])
@admin_required
def admin_reply_customer_message(message_id):
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}
        reply_message = to_str(body.get("reply_message"))

        if not reply_message:
            return json_error("Reply message is required.", 400)

        result = supabase_admin.table("customer_messages").update({
            "reply_message": reply_message
        }).eq("id", message_id).execute()

        if wants_json():
            return json_ok("Customer message reply saved.", rows_of(result))

        flash("Customer message reply saved.", "success")
        return redirect("/admin#messages")
    except Exception as e:
        print("CUSTOMER REPLY ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to save reply: {e}", "error")
        return redirect("/admin#messages")

@app.route("/admin/support/<ticket_id>/reply", methods=["POST"])
@admin_required
def admin_reply_support(ticket_id):
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}
        reply_message = to_str(body.get("reply_message"))
        status = to_str(body.get("status"), "OPEN")

        if not reply_message:
            return json_error("Reply message is required.", 400)

        result = supabase_admin.table("support_tickets").update({
            "reply_message": reply_message,
            "status": status
        }).eq("id", ticket_id).execute()

        if wants_json():
            return json_ok("Support ticket reply saved.", rows_of(result))

        flash("Support ticket reply saved.", "success")
        return redirect("/admin#support")
    except Exception as e:
        print("SUPPORT REPLY ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to save support reply: {e}", "error")
        return redirect("/admin#support")

@app.route("/admin/messages/<table_name>/<row_id>/delete", methods=["POST"])
@admin_required
def admin_delete_message_row(table_name, row_id):
    try:
        allowed = ["contacts", "customer_messages", "support_tickets"]
        if table_name not in allowed:
            return json_error("Invalid table.", 400)

        result = supabase_admin.table(table_name).delete().eq("id", row_id).execute()
        return json_ok("Record deleted successfully.", rows_of(result))
    except Exception as e:
        return json_error(e, 500)


# =========================================================
# ADMIN APPLICATIONS / APPROVALS / BLOCKING / DOCS
# =========================================================
@app.route("/api/admin/applications", methods=["GET"])
@admin_required
def api_admin_applications():
    try:
        table_name, apps = get_applications()
        return jsonify({
            "success": True,
            "source_table": table_name,
            "data": apps
        })
    except Exception as e:
        return json_error(e, 500)

@app.route("/api/admin/applications/<app_id>", methods=["GET"])
@admin_required
def api_admin_application_detail(app_id):
    try:
        table_name, row = get_application_by_id(app_id)
        if not row:
            return json_error("Application not found.", 404)

        row["documents_list"] = extract_documents(row)

        return jsonify({
            "success": True,
            "source_table": table_name,
            "data": row
        })
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/applications/<app_id>/approve", methods=["POST"])
@admin_required
def admin_approve_application(app_id):
    try:
        table_name, row = get_application_by_id(app_id)
        if not row:
            return json_error("Application not found.", 404)

        body = request.get_json(silent=True) or request.form.to_dict() or {}
        review_note = to_str(body.get("review_note"))

        result = supabase_admin.table(table_name).update({
            "status": "APPROVED",
            "review_note": review_note
        }).eq("id", app_id).execute()

        return json_ok("Application approved successfully.", rows_of(result))
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/applications/<app_id>/reject", methods=["POST"])
@admin_required
def admin_reject_application(app_id):
    try:
        table_name, row = get_application_by_id(app_id)
        if not row:
            return json_error("Application not found.", 404)

        body = request.get_json(silent=True) or request.form.to_dict() or {}
        review_note = to_str(body.get("review_note"))

        result = supabase_admin.table(table_name).update({
            "status": "REJECTED",
            "review_note": review_note
        }).eq("id", app_id).execute()

        return json_ok("Application rejected successfully.", rows_of(result))
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/applications/<app_id>/block", methods=["POST"])
@admin_required
def admin_block_application(app_id):
    try:
        table_name, row = get_application_by_id(app_id)
        if not row:
            return json_error("Application not found.", 404)

        body = request.get_json(silent=True) or request.form.to_dict() or {}
        review_note = to_str(body.get("review_note"), "Blocked by admin")

        result = supabase_admin.table(table_name).update({
            "status": "BLOCKED",
            "review_note": review_note
        }).eq("id", app_id).execute()

        email = row.get("email")
        phone = row.get("phone") or row.get("contact_number")

        if email or phone:
            blacklist_payload = {
                "application_id": app_id,
                "email": email,
                "phone": phone,
                "reason": review_note
            }
            try:
                supabase_admin.table("blacklist").insert(blacklist_payload).execute()
            except Exception as inner_e:
                print("BLACKLIST INSERT WARNING:", inner_e)

        return json_ok("Application blocked successfully.", rows_of(result))
    except Exception as e:
        return json_error(e, 500)

@app.route("/admin/applications/<app_id>/update", methods=["POST"])
@admin_required
def admin_update_application(app_id):
    try:
        table_name, row = get_application_by_id(app_id)
        if not row:
            return json_error("Application not found.", 404)

        body = request.get_json(silent=True) or request.form.to_dict() or {}

        payload = {}
        allowed_fields = [
            "full_name", "phone", "email", "contact_number", "whatsapp",
            "status", "amount", "term", "product_name", "review_note",
            "reply_message"
        ]

        for field in allowed_fields:
            if field in body:
                payload[field] = body.get(field)

        if not payload:
            return json_error("No valid fields to update.", 400)

        result = supabase_admin.table(table_name).update(payload).eq("id", app_id).execute()
        return json_ok("Application updated successfully.", rows_of(result))
    except Exception as e:
        return json_error(e, 500)


# =========================================================
# SITE SETTINGS / INDEX REFLECTION
# =========================================================
@app.route("/admin/site-config/save", methods=["POST"])
@admin_required
def admin_save_site_config():
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}
        payload = {
            "site_name": body.get("site_name"),
            "promo_text": body.get("promo_text"),
            "hero_title": body.get("hero_title"),
            "hero_subtitle": body.get("hero_subtitle"),
            "hero_trust_text": body.get("hero_trust_text"),
            "hero_chip_1": body.get("hero_chip_1"),
            "hero_chip_2": body.get("hero_chip_2"),
            "hero_chip_3": body.get("hero_chip_3"),
            "support_phone": body.get("support_phone"),
            "email": body.get("email"),
            "approval_window": body.get("approval_window"),
            "calc_note": body.get("calc_note"),
            "min_loan": body.get("min_loan"),
            "max_loan": body.get("max_loan"),
            "office_hours": body.get("office_hours"),
            "footer_text": body.get("footer_text")
        }

        clean = {}
        for k, v in payload.items():
            if v is not None:
                clean[k] = v

        result = upsert_singleton("site_config", clean)

        if wants_json():
            return json_ok("Site config saved successfully.", rows_of(result))

        flash("Site config saved successfully.", "success")
        return redirect(url_for("admin_dashboard"))
    except Exception as e:
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to save site config: {e}", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/site-settings/save", methods=["POST"])
@admin_required
def admin_save_site_settings():
    try:
        body = request.get_json(silent=True) or request.form.to_dict() or {}

        payload = {
            "interest_rate": to_float(body.get("interest_rate"), 0),
            "service_fee": to_float(body.get("service_fee"), 0),
            "business_loan_rate": to_float(body.get("business_loan_rate"), 0),
            "personal_loan_rate": to_float(body.get("personal_loan_rate"), 0),
            "salary_advance_rate": to_float(body.get("salary_advance_rate"), 0)
        }

        result = upsert_singleton("site_settings", payload)

        if wants_json():
            return json_ok("Site settings saved successfully.", rows_of(result))

        flash("Site settings saved successfully.", "success")
        return redirect(url_for("admin_dashboard"))
    except Exception as e:
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to save site settings: {e}", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/site-settings/apply-products", methods=["POST"])
@admin_required
def admin_apply_site_settings_to_products():
    try:
        settings = safe_first("site_settings")
        if not settings:
            return json_error("No site settings found. Save settings first.", 400)

        products = safe_rows("loan_products", "id", False)
        updated_count = 0

        for p in products:
            name = to_str(p.get("name")).lower()
            rate = settings.get("interest_rate", 0)

            if "business" in name:
                rate = settings.get("business_loan_rate", rate)
            elif "personal" in name:
                rate = settings.get("personal_loan_rate", rate)
            elif "salary" in name:
                rate = settings.get("salary_advance_rate", rate)

            supabase_admin.table("loan_products").update({
                "interest_rate": to_float(rate, 0),
                "service_fee": to_float(settings.get("service_fee", 0), 0)
            }).eq("id", p.get("id")).execute()
            updated_count += 1

        return json_ok(f"Rates applied to {updated_count} product(s).")
    except Exception as e:
        return json_error(e, 500)

# =========================================================
# OVERVIEW DATA
# =========================================================
@app.route("/api/admin/overview")
@admin_required
def api_admin_overview():
    try:
        table_name, applications = get_applications()
        loan_products = safe_rows("loan_products")
        support_tickets = safe_rows("support_tickets")
        customer_messages = safe_rows("customer_messages")

        pending = len([x for x in applications if str(x.get("status")).upper()=="PENDING"])
        approved = len([x for x in applications if str(x.get("status")).upper()=="APPROVED"])
        rejected = len([x for x in applications if str(x.get("status")).upper()=="REJECTED"])

        total_requested = sum([float(x.get("amount") or 0) for x in applications])

        return jsonify({
            "success": True,
            "counts": {
                "applications": len(applications),
                "pending": pending,
                "approved": approved,
                "rejected": rejected,
                "loan_products": len(loan_products),
                "total_requested": total_requested
            },
            "recent_applications": applications[:10]
        })
    except Exception as e:
        return json_error(e)


# =========================================================
# SITE SETTINGS LOAD
# =========================================================
@app.route("/api/admin/site-settings")
@admin_required
def api_admin_site_settings():
    try:
        settings = safe_first("site_settings")
        return json_ok(data=settings)
    except Exception as e:
        return json_error(e)


# =========================================================
# SEND ADMIN REPLY
# =========================================================
@app.route("/admin/messages/reply", methods=["POST"])
@admin_required
def admin_send_reply():
    try:
        body = request.get_json()

        payload = {
            "customer_id": body.get("customer_id"),
            "subject": body.get("subject"),
            "message": body.get("message"),
            "sender_role": "admin"
        }

        supabase_admin.table("customer_messages").insert(payload).execute()

        return json_ok("Reply sent successfully")
    except Exception as e:
        return json_error(e)


# =========================================================
# DEBUG
# =========================================================
@app.route("/debug/routes")
def debug_routes():
    try:
        lines = []
        for rule in sorted(app.url_map.iter_rules(), key=lambda r: str(r.rule)):
            methods = ",".join(sorted([m for m in rule.methods if m not in ["HEAD", "OPTIONS"]]))
            lines.append(f"{rule.rule:55} | {methods:20} | endpoint={rule.endpoint}")
        return "<pre>" + "\n".join(lines) + "</pre>"
    except Exception as e:
        return f"ROUTE DEBUG ERROR: {e}", 500

def print_all_routes():
    print("")
    print("=" * 110)
    print("REGISTERED ROUTES")
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: str(r.rule)):
        methods = ",".join(sorted([m for m in rule.methods if m not in ["HEAD", "OPTIONS"]]))
        print(f"{rule.rule:55} | {methods:20} | endpoint={rule.endpoint}")
    print("=" * 110)
    print("")


# =========================================================
# CUSTOMER PORTAL / APPLICATION SUBMIT / WAITING DASHBOARD
# =========================================================
import uuid
from datetime import datetime, timezone

def _safe_filename(name):
    name = (name or "").strip()
    name = name.replace("\\", "_").replace("/", "_").replace(":", "_")
    name = name.replace("*", "_").replace("?", "_").replace('"', "_")
    name = name.replace("<", "_").replace(">", "_").replace("|", "_")
    return name

def _ensure_upload_dir():
    upload_dir = os.path.join(BASE_DIR, "static", "uploads", "applications")
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir

def _save_upload(file_obj, prefix):
    try:
        if not file_obj or not getattr(file_obj, "filename", None):
            return ""
        filename = _safe_filename(file_obj.filename)
        if not filename:
            return ""
        ext = os.path.splitext(filename)[1]
        unique_name = f"{prefix}_{uuid.uuid4().hex}{ext}"
        upload_dir = _ensure_upload_dir()
        abs_path = os.path.join(upload_dir, unique_name)
        file_obj.save(abs_path)
        return f"/static/uploads/applications/{unique_name}"
    except Exception as e:
        print("FILE SAVE ERROR:", e)
        return ""


def _table_columns(table_name):
    try:
        # use a very small known-safe payload check strategy
        sample = supabase_admin.table(table_name).select("*").limit(1).execute()
        rows = getattr(sample, "data", None) or []
        if rows and isinstance(rows, list) and len(rows) > 0:
            return set(rows[0].keys())
    except Exception as e:
        print("TABLE COLUMN READ ERROR:", table_name, e)
    return set()

def _filter_payload_to_existing_columns(table_name, payload):
    cols = _table_columns(table_name)
    if cols:
        return {k: v for k, v in payload.items() if k in cols}

    # fallback allow-list for common applications columns when table is empty
    common_application_cols = {
        "reference","ref","full_name","phone","email","id_number","date_of_birth","gender",
        "physical_address","town_city","region","employment_status","employer_name",
        "monthly_income","other_income","amount","loan_amount","term","loan_term",
        "product_name","loan_purpose","next_of_kin_name","next_of_kin_phone",
        "next_of_kin_relationship","next_of_kin_address","geo_lat","geo_lng",
        "geo_accuracy","geo_timestamp","device_type","user_agent","platform_info",
        "screen_info","timezone_info","language_info","face_capture_data",
        "id_front_url","id_back_url","bank_statement_url","proof_of_income_url",
        "proof_of_address_url","supporting_doc_url","documents","status",
        "review_note","reply_message","created_at"
    }
    return {k: v for k, v in payload.items() if k in common_application_cols}

def clean_float_or_none(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def clean_int_or_zero(v):
    try:
        if v is None:
            return 0
        s = str(v).strip()
        if s == "":
            return 0
        return int(float(s))
    except Exception:
        return 0


def clean_timestamp_or_none(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return s
    except Exception:
        return None

def _first_existing_table(*names):
    for name in names:
        try:
            supabase_admin.table(name).select("id").limit(1).execute()
            return name
        except Exception:
            continue
    return names[0] if names else None

def _load_customer_profile(email=None, phone=None):
    try:
        rows = safe_rows("customer_profiles")
        for row in rows:
            row_email = str(row.get("email") or "").strip().lower()
            row_phone = str(row.get("phone") or "").strip()
            if email and row_email == str(email).strip().lower():
                return row
            if phone and row_phone == str(phone).strip():
                return row
    except Exception as e:
        print("LOAD CUSTOMER PROFILE ERROR:", e)
    return {}

def _load_customer_applications(email=None, phone=None):
    table_name = _first_existing_table("applications", "loan_applications")
    try:
        rows = safe_rows(table_name, "created_at", True)
        filtered = []
        for row in rows:
            row_email = str(row.get("email") or "").strip().lower()
            row_phone = str(row.get("phone") or row.get("contact_number") or "").strip()
            if email and row_email == str(email).strip().lower():
                filtered.append(row)
            elif phone and row_phone == str(phone).strip():
                filtered.append(row)
        return table_name, filtered
    except Exception as e:
        print("LOAD CUSTOMER APPLICATIONS ERROR:", e)
        return table_name, []

def _build_status_history(applications):
    history = []
    for app in applications:
        history.append({
            "status": app.get("status") or "PENDING",
            "note": app.get("review_note") or app.get("reply_message") or "Application received and waiting for admin review.",
            "created_at": app.get("created_at") or ""
        })
    history.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return history

def _customer_messages_for(email=None, phone=None):
    rows = safe_rows("customer_messages", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        row_customer_id = str(row.get("customer_id") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
        elif email and row_customer_id == str(email).strip().lower():
            out.append(row)
    return out

def _support_tickets_for(email=None, phone=None):
    rows = safe_rows("support_tickets", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("customer_email") or row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out

def _loan_accounts_for(email=None, phone=None):
    rows = safe_rows("loan_accounts", "opened_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out

def _loan_payments_for(email=None, phone=None):
    rows = safe_rows("loan_payments", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out

@app.route("/customer/secure-apply", methods=["GET", "POST"])
def customer_secure_apply():
    if request.method == "GET":
        return redirect(url_for("apply"))

    try:
        form = request.form

        full_name = to_str(form.get("full_name"))
        phone = to_str(form.get("phone"))
        email = to_str(form.get("email")).lower()
        id_number = to_str(form.get("id_number"))
        loan_amount = clean_int_or_zero(form.get("loan_amount"))
        loan_term = to_str(form.get("loan_term"))
        loan_purpose = to_str(form.get("loan_purpose"))
        employment_status = to_str(form.get("employment_status"))
        employer_name = to_str(form.get("employer_name"))
        monthly_income = clean_int_or_zero(form.get("monthly_income"))
        other_income = clean_int_or_zero(form.get("other_income"))

        reference = "KATRA-" + datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:6].upper()

        id_front_url = _save_upload(request.files.get("id_front"), "id_front")
        id_back_url = _save_upload(request.files.get("id_back"), "id_back")
        bank_statement_url = _save_upload(request.files.get("bank_statement"), "bank_statement")
        proof_of_income_url = _save_upload(request.files.get("proof_of_income"), "proof_of_income")
        proof_of_address_url = _save_upload(request.files.get("proof_of_address"), "proof_of_address")
        supporting_doc_url = _save_upload(request.files.get("supporting_doc"), "supporting_doc")

        documents = []
        if id_front_url:
            documents.append({"label": "id_front", "url": id_front_url})
        if id_back_url:
            documents.append({"label": "id_back", "url": id_back_url})
        if bank_statement_url:
            documents.append({"label": "bank_statement", "url": bank_statement_url})
        if proof_of_income_url:
            documents.append({"label": "proof_of_income", "url": proof_of_income_url})
        if proof_of_address_url:
            documents.append({"label": "proof_of_address", "url": proof_of_address_url})
        if supporting_doc_url:
            documents.append({"label": "supporting_doc", "url": supporting_doc_url})

        app_table = _first_existing_table("applications", "loan_applications")

        payload = {
            "reference": reference,
            "ref": reference,
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "id_number": id_number,
            "date_of_birth": to_str(form.get("date_of_birth")),
            "gender": to_str(form.get("gender")),
            "physical_address": to_str(form.get("physical_address")),
            "town_city": to_str(form.get("town_city")),
            "region": to_str(form.get("region")),
            "employment_status": employment_status,
            "employer_name": employer_name,
            "monthly_income": int(monthly_income),
            "other_income": int(other_income),
            "amount": int(loan_amount),
            "loan_amount": int(loan_amount),
            "term": loan_term,
            "loan_term": loan_term,
            "product_name": "General Loan Application",
            "loan_purpose": loan_purpose,
            "next_of_kin_name": to_str(form.get("next_of_kin_name")),
            "next_of_kin_phone": to_str(form.get("next_of_kin_phone")),
            "next_of_kin_relationship": to_str(form.get("next_of_kin_relationship")),
            "next_of_kin_address": to_str(form.get("next_of_kin_address")),
            "geo_lat": clean_float_or_none(form.get("geo_lat")),
            "geo_lng": clean_float_or_none(form.get("geo_lng")),
            "geo_accuracy": clean_float_or_none(form.get("geo_accuracy")),
            "geo_timestamp": clean_timestamp_or_none(form.get("geo_timestamp")),
            "device_type": to_str(form.get("device_type")),
            "user_agent": to_str(form.get("user_agent")),
            "platform_info": to_str(form.get("platform_info")),
            "screen_info": to_str(form.get("screen_info")),
            "timezone_info": to_str(form.get("timezone_info")),
            "language_info": to_str(form.get("language_info")),
            "face_capture_data": to_str(form.get("face_capture_data")),
            "id_front_url": id_front_url,
            "id_back_url": id_back_url,
            "bank_statement_url": bank_statement_url,
            "proof_of_income_url": proof_of_income_url,
            "proof_of_address_url": proof_of_address_url,
            "supporting_doc_url": supporting_doc_url,
            "documents": documents,
            "status": "SUBMITTED",
            "review_note": "Application submitted. Waiting for admin review.",
            "reply_message": "Your application has been received and is under review."
        }

        safe_payload = _filter_payload_to_existing_columns(app_table, payload)
        supabase_admin.table(app_table).insert(safe_payload).execute()

        profile_payload = {
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "physical_address": to_str(form.get("physical_address")),
            "town_city": to_str(form.get("town_city")),
            "region": to_str(form.get("region"))
        }

        try:
            existing_profile = _load_customer_profile(email=email, phone=phone)
            if existing_profile and existing_profile.get("id"):
                supabase_admin.table("customer_profiles").update(profile_payload).eq("id", existing_profile["id"]).execute()
            else:
                supabase_admin.table("customer_profiles").insert(profile_payload).execute()
        except Exception as e:
            print("CUSTOMER PROFILE UPSERT WARNING:", e)

        session["customer_email"] = email
        session["customer_phone"] = phone
        session["customer_full_name"] = full_name
        session["customer_reference"] = reference

        return redirect(url_for("customer_dashboard"))

    except Exception as e:
        print("CUSTOMER SECURE APPLY ERROR:", e)
        return f"APPLICATION SUBMIT ERROR: {e}", 500


@app.route("/customer/progress-login", methods=["GET", "POST"])
def customer_progress_login():
    error = None

    if request.method == "POST":
        email = to_str(request.form.get("email")).strip().lower()
        phone = to_str(request.form.get("phone")).strip()

        if not email or not phone:
            error = "Enter your email and phone number."
        else:
            app_table, applications = _load_customer_applications(email=email, phone=phone)

            matched = None
            for row in applications:
                row_email = str(row.get("email") or "").strip().lower()
                row_phone = str(row.get("phone") or row.get("contact_number") or "").strip()
                if row_email == email and row_phone == phone:
                    matched = row
                    break

            if matched:
                session["customer_email"] = email
                session["customer_phone"] = phone
                session["customer_full_name"] = matched.get("full_name") or ""
                session["customer_reference"] = matched.get("reference") or matched.get("ref") or ""
                return redirect(url_for("customer_dashboard"))
            else:
                error = "No matching application found for that email and phone number."

    return render_template("customer_progress_login.html", error=error)


@app.route("/customer/dashboard")
def customer_dashboard():
    email = session.get("customer_email")
    phone = session.get("customer_phone")

    if not email and not phone:
        return redirect(url_for("apply"))

    app_table, applications = _load_customer_applications(email=email, phone=phone)
    profile = _load_customer_profile(email=email, phone=phone)
    loan_accounts = _loan_accounts_for(email=email, phone=phone)
    loan_payments = _loan_payments_for(email=email, phone=phone)
    customer_messages = _customer_messages_for(email=email, phone=phone)
    support_tickets = _support_tickets_for(email=email, phone=phone)
    status_history = _build_status_history(applications)

    current_status = "PENDING"
    approved_amount = 0
    if applications:
        latest = applications[0]
        current_status = str(latest.get("status") or "PENDING").upper()
        approved_amount = latest.get("amount") or 0

    paid_amount = 0
    try:
        paid_amount = sum([to_float(x.get("amount") or x.get("paid_amount") or 0, 0) for x in loan_payments])
    except Exception:
        paid_amount = 0

    balance_amount = max(to_float(approved_amount, 0) - to_float(paid_amount, 0), 0)

    repayment_schedule = []
    for idx, acc in enumerate(loan_accounts, start=1):
        repayment_schedule.append({
            "installment_no": idx,
            "due_date": acc.get("next_due_date") or acc.get("due_date") or "",
            "due_amount": acc.get("installment_amount") or acc.get("monthly_due") or 0,
            "paid_amount": acc.get("paid_amount") or 0,
            "status": acc.get("status") or "PENDING"
        })

    return render_template(
        "customer_dashboard.html",
        profile=profile,
        customer_email=email,
        applications=applications,
        status_history=status_history,
        repayment_schedule=repayment_schedule,
        customer_messages=customer_messages,
        support_tickets=support_tickets,
        loan_accounts=loan_accounts,
        current_status=current_status,
        approved_amount=approved_amount,
        paid_amount=paid_amount,
        balance_amount=balance_amount,
        dashboard_ad_headline="Your application is being reviewed",
        dashboard_ad_text="Track your application, support, and repayments from one secure portal.",
        powered_by_text="KATRAMONEY Secure Customer Portal",
        developer_credit="Application tracking, support, and loan status center.",
        customer_wallpaper_url=""
    )

@app.route("/customer/logout")
def customer_logout():
    session.pop("customer_email", None)
    session.pop("customer_phone", None)
    session.pop("customer_full_name", None)
    session.pop("customer_reference", None)
    return redirect(url_for("apply"))

@app.route("/customer/support/new", methods=["POST"])
def customer_support_new():
    try:
        email = session.get("customer_email") or ""
        phone = session.get("customer_phone") or ""
        full_name = session.get("customer_full_name") or ""

        payload = {
            "customer_name": full_name,
            "customer_email": email,
            "phone": phone,
            "subject": to_str(request.form.get("subject")),
            "message": to_str(request.form.get("message")),
            "status": "OPEN"
        }
        safe_payload = _filter_payload_to_existing_columns("support_tickets", payload)
        supabase_admin.table("support_tickets").insert(safe_payload).execute()
        return redirect(url_for("customer_dashboard"))
    except Exception as e:
        print("CUSTOMER SUPPORT NEW ERROR:", e)
        return f"SUPPORT TICKET ERROR: {e}", 500

@app.route("/customer/profile/save", methods=["POST"])
def customer_profile_save():
    try:
        email = session.get("customer_email") or ""
        phone = session.get("customer_phone") or ""

        payload = {
            "full_name": to_str(request.form.get("full_name")),
            "phone": to_str(request.form.get("phone")),
            "email": email,
            "physical_address": to_str(request.form.get("physical_address")),
            "town_city": to_str(request.form.get("town_city")),
            "region": to_str(request.form.get("region"))
        }

        existing = _load_customer_profile(email=email, phone=phone)
        if existing and existing.get("id"):
            supabase_admin.table("customer_profiles").update(payload).eq("id", existing["id"]).execute()
        else:
            supabase_admin.table("customer_profiles").insert(payload).execute()

        if payload.get("phone"):
            session["customer_phone"] = payload["phone"]
        if payload.get("full_name"):
            session["customer_full_name"] = payload["full_name"]

        return redirect(url_for("customer_dashboard"))
    except Exception as e:
        print("CUSTOMER PROFILE SAVE ERROR:", e)
        return f"PROFILE SAVE ERROR: {e}", 500

@app.route("/customer/payment-proof/upload", methods=["POST"])
def customer_payment_proof_upload():
    try:
        email = session.get("customer_email") or ""
        phone = session.get("customer_phone") or ""

        proof_url = _save_upload(request.files.get("payment_proof"), "payment_proof")
        payload = {
            "loan_account_id": to_str(request.form.get("loan_account_id")),
            "email": email,
            "phone": phone,
            "amount": to_float(request.form.get("payment_amount"), 0),
            "payment_method": to_str(request.form.get("payment_method")),
            "payment_note": to_str(request.form.get("payment_note")),
            "payment_proof_url": proof_url,
            "status": "SUBMITTED"
        }
        supabase_admin.table("loan_payments").insert(payload).execute()
        return redirect(url_for("customer_dashboard"))
    except Exception as e:
        print("PAYMENT PROOF ERROR:", e)
        return f"PAYMENT PROOF ERROR: {e}", 500



# --- duplicate application identity check ---

def _norm_email(v):
    return str(v or "").strip().lower()

def _norm_phone(v):
    raw = str(v or "").strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("0") and len(digits) >= 9:
        digits = "264" + digits[1:]
    return digits

def _norm_id(v):
    return re.sub(r"\s+", "", str(v or "").strip()).upper()

def _supabase_rest_config():
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )
    return url, key

def _rest_headers(key):
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

def _rest_exists(base_url, key, table, column, value):
    if not base_url or not key or not table or not column or value in (None, ""):
        return False
    url = f"{base_url}/rest/v1/{table}"
    params = {
        "select": "id",
        column: f"eq.{value}",
        "limit": "1"
    }
    r = requests.get(url, headers=_rest_headers(key), params=params, timeout=12)
    if r.status_code >= 400:
        raise Exception(f"{table}.{column} -> HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    return bool(data)

@app.route("/api/check-application-identity", methods=["POST"])
def check_application_identity():
    payload = request.get_json(silent=True) or {}

    email = _norm_email(payload.get("email"))
    phone = _norm_phone(payload.get("phone"))
    id_number = _norm_id(payload.get("id_number"))

    if not email and not phone and not id_number:
        return jsonify({"exists": False})

    base_url, key = _supabase_rest_config()
    if not base_url or not key:
        return jsonify({
            "exists": False,
            "error": "SUPABASE_URL or key missing on server."
        }), 500

    # Put your real applications table first.
    tables_to_try = [
        "applications",
        "loan_applications",
        "customer_applications"
    ]

    email_columns = ["email"]
    phone_columns = ["phone", "cellphone", "phone_number"]
    id_columns = ["id_number", "id_no", "national_id"]

    matched_fields = []
    debug_errors = []

    for table in tables_to_try:
        try:
            if email:
                for col in email_columns:
                    try:
                        if _rest_exists(base_url, key, table, col, email):
                            matched_fields.append("email")
                            break
                    except Exception as e:
                        debug_errors.append(str(e))

            if phone:
                for col in phone_columns:
                    try:
                        if _rest_exists(base_url, key, table, col, phone):
                            matched_fields.append("phone number")
                            break
                    except Exception as e:
                        debug_errors.append(str(e))

            if id_number:
                for col in id_columns:
                    try:
                        if _rest_exists(base_url, key, table, col, id_number):
                            matched_fields.append("ID number")
                            break
                    except Exception as e:
                        debug_errors.append(str(e))

            if matched_fields:
                matched_fields = list(dict.fromkeys(matched_fields))
                fields_text = ", ".join(matched_fields)
                return jsonify({
                    "exists": True,
                    "matched_fields": matched_fields,
                    "message": f"This {fields_text} was once used. Please go back to the home page, click View My Application Progress, and log in using the same email and your phone number as password."
                })

        except Exception as e:
            debug_errors.append(str(e))

    # If every table/column failed, return a real error so frontend can show it clearly
    if debug_errors and not matched_fields:
        return jsonify({
            "exists": False,
            "error": "Duplicate verification failed on server.",
            "details": debug_errors[:8]
        }), 500

    return jsonify({"exists": False})

# --- end duplicate application identity check ---




@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": "katramoney",
        "status": "up"
    }), 200




# ===== Admin security / visitors / user reset =====

def _sb_url():
    return os.getenv("SUPABASE_URL", "").rstrip("/")

def _sb_key():
    return (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )

def _sb_headers():
    key = _sb_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

def _sb_get(table, params=None):
    url = f"{_sb_url()}/rest/v1/{table}"
    r = requests.get(url, headers=_sb_headers(), params=params or {}, timeout=20)
    return r

def _sb_post(table, payload):
    url = f"{_sb_url()}/rest/v1/{table}"
    r = requests.post(url, headers=_sb_headers(), json=payload, timeout=20)
    return r

def _sb_patch(table, match_params, payload):
    url = f"{_sb_url()}/rest/v1/{table}"
    r = requests.patch(url, headers=_sb_headers(), params=match_params or {}, json=payload, timeout=20)
    return r

def _sb_safe_json(res):
    try:
        return res.json()
    except Exception:
        return {}

def _admin_session_email():
    return (
        session.get("admin_email")
        or session.get("email")
        or session.get("user_email")
        or ""
    )

def _admin_required_json(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _admin_session_email():
            return jsonify({"success": False, "error": "Admin login required."}), 401
        return fn(*args, **kwargs)
    return wrapper

def _first_row(data):
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return {}

def _find_admin_profile(email):
    if not email:
        return {}
    res = _sb_get("admin_profiles", {
        "select": "*",
        "email": f"eq.{email}",
        "limit": "1"
    })
    data = _sb_safe_json(res)
    return _first_row(data)

def _find_customer_rows(q=""):
    params = {"select": "*", "limit": "50", "order": "created_at.desc"}
    res = _sb_get("customer_profiles", params)
    data = _sb_safe_json(res)
    rows = data if isinstance(data, list) else []

    if q:
        ql = str(q).strip().lower()
        rows = [
            r for r in rows
            if ql in str(r.get("full_name", "")).lower()
            or ql in str(r.get("email", "")).lower()
            or ql in str(r.get("phone", "")).lower()
        ]
    return rows

@app.route("/api/admin/security/profile", methods=["GET"])
@_admin_required_json
def api_admin_security_profile():
    email = _admin_session_email()
    row = _find_admin_profile(email)

    if not row:
        return jsonify({
            "success": True,
            "data": {
                "email": email,
                "phone": "",
                "updated_at": ""
            }
        })

    return jsonify({
        "success": True,
        "data": {
            "email": row.get("email", email),
            "phone": row.get("phone", ""),
            "updated_at": row.get("updated_at") or row.get("created_at") or ""
        }
    })

@app.route("/admin/security/update-contact", methods=["POST"])
@_admin_required_json
def admin_update_contact():
    payload = request.get_json(silent=True) or {}
    phone = str(payload.get("phone", "") or "").strip()
    email = _admin_session_email()

    if not phone:
        return jsonify({"success": False, "error": "Phone number is required."}), 400

    existing = _find_admin_profile(email)

    if existing and existing.get("id"):
        res = _sb_patch("admin_profiles", {"id": f"eq.{existing['id']}"}, {
            "phone": phone,
            "updated_at": datetime.now(timezone.utc).isoformat()
        })
    else:
        res = _sb_post("admin_profiles", {
            "email": email,
            "phone": phone,
            "updated_at": datetime.now(timezone.utc).isoformat()
        })

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to update support number.",
            "details": res.text[:400]
        }), 500

    return jsonify({"success": True, "message": "Support number updated."})

@app.route("/admin/security/change-password", methods=["POST"])
@_admin_required_json
def admin_change_password():
    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("current_password", "") or "")
    new_password = str(payload.get("new_password", "") or "")
    email = _admin_session_email()

    if not current_password or not new_password:
        return jsonify({"success": False, "error": "Current and new password are required."}), 400

    if len(new_password) < 6:
        return jsonify({"success": False, "error": "New password must be at least 6 characters."}), 400

    row = _find_admin_profile(email)
    if not row:
        return jsonify({"success": False, "error": "Admin profile not found."}), 404

    stored_hash = row.get("password_hash", "")
    if not stored_hash:
        return jsonify({"success": False, "error": "Admin password hash not configured in admin_profiles."}), 400

    if not check_password_hash(stored_hash, current_password):
        return jsonify({"success": False, "error": "Current password is incorrect."}), 400

    new_hash = generate_password_hash(new_password)
    res = _sb_patch("admin_profiles", {"id": f"eq.{row['id']}"}, {
        "password_hash": new_hash,
        "updated_at": datetime.now(timezone.utc).isoformat()
    })

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to update password.",
            "details": res.text[:400]
        }), 500

    return jsonify({"success": True, "message": "Admin password updated."})

@app.route("/api/admin/customers", methods=["GET"])
@_admin_required_json
def api_admin_customers():
    q = request.args.get("q", "").strip()
    try:
        rows = _find_customer_rows(q)
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to load customers: {e}"}), 500

    data = [{
        "id": r.get("id"),
        "full_name": r.get("full_name", ""),
        "email": r.get("email", ""),
        "phone": r.get("phone", ""),
        "status": r.get("status", ""),
        "last_login": r.get("last_login", "")
    } for r in rows]

    return jsonify({"success": True, "data": data})

@app.route("/admin/customers/<user_id>/reset-access", methods=["POST"])
@_admin_required_json
def admin_reset_customer_access(user_id):
    if not user_id:
        return jsonify({"success": False, "error": "User id required."}), 400

    payload = {
        "reset_required": True,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    res = _sb_patch("customer_profiles", {"id": f"eq.{user_id}"}, payload)

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to reset user access.",
            "details": res.text[:400]
        }), 500

    return jsonify({"success": True, "message": "User access reset. User can now be helped with account recovery."})

@app.route("/api/admin/visitors", methods=["GET"])
@_admin_required_json
def api_admin_visitors():
    res = _sb_get("visitor_logs", {
        "select": "*",
        "order": "created_at.desc",
        "limit": "100"
    })

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to load visitor logs.",
            "details": res.text[:400]
        }), 500

    rows = _sb_safe_json(res)
    if not isinstance(rows, list):
        rows = []

    data = [{
        "id": r.get("id"),
        "name": r.get("name", ""),
        "email": r.get("email", ""),
        "phone": r.get("phone", ""),
        "device_type": r.get("device_type", ""),
        "browser": r.get("browser", ""),
        "user_agent": r.get("user_agent", ""),
        "platform": r.get("platform", ""),
        "location": r.get("location", ""),
        "region": r.get("region", ""),
        "city": r.get("city", ""),
        "ip_address": r.get("ip_address", ""),
        "last_seen": r.get("last_seen", ""),
        "created_at": r.get("created_at", "")
    } for r in rows]

    return jsonify({"success": True, "data": data})

@app.before_request
def track_visitors_for_admin():
    try:
        # Keep this lightweight and skip obvious static/internal routes
        path = request.path or ""
        if request.method != "GET":
            return
        if path.startswith("/static") or path.startswith("/favicon") or path.startswith("/health"):
            return

        ua = request.headers.get("User-Agent", "")
        ip_address = (
            request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.headers.get("X-Real-IP", "").strip()
            or request.remote_addr
            or ""
        )

        browser = "Unknown"
        ua_low = ua.lower()
        if "chrome" in ua_low and "edg" not in ua_low:
            browser = "Chrome"
        elif "safari" in ua_low and "chrome" not in ua_low:
            browser = "Safari"
        elif "firefox" in ua_low:
            browser = "Firefox"
        elif "edg" in ua_low:
            browser = "Edge"

        device_type = "Desktop"
        if "iphone" in ua_low or "ipad" in ua_low:
            device_type = "iPhone/iPad"
        elif "android" in ua_low:
            device_type = "Android"
        elif "mobile" in ua_low:
            device_type = "Mobile"

        platform = request.headers.get("Sec-CH-UA-Platform", "") or ""

        payload = {
            "ip_address": ip_address,
            "user_agent": ua[:500],
            "browser": browser,
            "device_type": device_type,
            "platform": platform,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "path": path
        }

        # Optional session-based identity
        if session.get("customer_email"):
            payload["email"] = session.get("customer_email")
        if session.get("customer_phone"):
            payload["phone"] = session.get("customer_phone")

        # Insert only; admin page can review latest activity
        _sb_post("visitor_logs", payload)
    except Exception:
        pass

# ===== End admin security / visitors / user reset =====




@app.route("/admin/applications/delete-old", methods=["POST"])
def admin_delete_old_applications():
    admin_email = session.get("admin_email") or session.get("email") or session.get("user_email")
    if not admin_email:
        return jsonify({"success": False, "error": "Admin login required."}), 401

    payload = request.get_json(silent=True) or {}
    days_old = int(payload.get("days_old") or 0)
    status = str(payload.get("status") or "").strip().upper()

    if days_old < 1:
        return jsonify({"success": False, "error": "days_old must be at least 1."}), 400

    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_old)).isoformat()

    base_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )

    if not base_url or not key:
        return jsonify({"success": False, "error": "Supabase config missing."}), 500

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

    table = "applications"
    params = {
        "created_at": f"lt.{cutoff}"
    }

    if status:
        params["status"] = f"eq.{status}"

    import requests
    res = requests.delete(
        f"{base_url}/rest/v1/{table}",
        headers=headers,
        params=params,
        timeout=30
    )

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to delete old applications.",
            "details": res.text[:500]
        }), 500

    return jsonify({
        "success": True,
        "message": f"Old applications deleted successfully."
    })




@app.route("/admin/applications/<app_id>/delete", methods=["POST"])
def admin_delete_application(app_id):
    admin_email = session.get("admin_email") or session.get("email") or session.get("user_email")
    if not admin_email:
        return jsonify({"success": False, "error": "Admin login required."}), 401

    if not app_id:
        return jsonify({"success": False, "error": "Application id required."}), 400

    base_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )

    if not base_url or not key:
        return jsonify({"success": False, "error": "Supabase config missing."}), 500

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

    import requests
    table = "applications"
    res = requests.delete(
        f"{base_url}/rest/v1/{table}",
        headers=headers,
        params={"id": f"eq.{app_id}"},
        timeout=30
    )

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to delete application.",
            "details": res.text[:500]
        }), 500

    return jsonify({
        "success": True,
        "message": "Application deleted successfully."
    })




# ===== Reports / Application profile PDF =====

def _admin_auth_email():
    return session.get("admin_email") or session.get("email") or session.get("user_email") or ""

def _sb_base_url():
    return os.getenv("SUPABASE_URL", "").rstrip("/")

def _sb_service_key():
    return (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )

def _sb_headers_json():
    key = _sb_service_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

def _sb_headers_file():
    key = _sb_service_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}"
    }

def _require_admin_report():
    if not _admin_auth_email():
        return False
    return True

def _rest_get_rows(table, params=None):
    import requests
    url = f"{_sb_base_url()}/rest/v1/{table}"
    r = requests.get(url, headers=_sb_headers_json(), params=params or {}, timeout=30)
    if r.status_code >= 400:
        raise Exception(r.text[:500])
    try:
        data = r.json()
    except Exception:
        data = []
    return data if isinstance(data, list) else []

def _rest_get_one(table, params=None):
    rows = _rest_get_rows(table, params)
    return rows[0] if rows else {}

def _guess_document_urls(app_row):
    docs = []

    # direct list already prepared
    if isinstance(app_row.get("documents_list"), list):
        for d in app_row.get("documents_list") or []:
            if isinstance(d, dict) and d.get("url"):
                docs.append({
                    "label": d.get("label") or "Document",
                    "url": d.get("url")
                })

    # common flat columns
    common_cols = [
        ("Certified ID Copy", "id_copy_url"),
        ("Bank Statement", "bank_statement_url"),
        ("Payslip", "payslip_url"),
        ("Proof of Address", "proof_of_address_url"),
        ("Founding Statement", "founding_statement_url"),
        ("Request Letter", "request_letter_url"),
        ("Face Capture", "face_capture_url"),
        ("Selfie", "selfie_url"),
    ]

    for label, col in common_cols:
        val = app_row.get(col)
        if val:
            docs.append({"label": label, "url": val})

    # optional uploaded_documents array
    if isinstance(app_row.get("uploaded_documents"), list):
        for d in app_row.get("uploaded_documents") or []:
            if isinstance(d, dict) and d.get("url"):
                docs.append({
                    "label": d.get("label") or d.get("document_type") or "Uploaded Document",
                    "url": d.get("url")
                })

    # dedupe
    seen = set()
    final_docs = []
    for d in docs:
        key = (d.get("label"), d.get("url"))
        if key in seen:
            continue
        seen.add(key)
        final_docs.append(d)
    return final_docs

def _download_file_bytes(url):
    import requests
    r = requests.get(url, headers=_sb_headers_file(), timeout=60)
    if r.status_code >= 400:
        raise Exception(f"Failed to download file: {r.status_code}")
    return r.content

def _make_cover_pdf_bytes(app_row, docs):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    y = height - 50
    c.setFont("Helvetica-Bold", 18)
    c.drawString(40, y, "KATRAMONEY Applicant Profile Pack")

    y -= 24
    c.setFont("Helvetica", 10)
    c.drawString(40, y, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    y -= 28
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y, "Applicant Profile")

    info = [
        ("Reference", app_row.get("reference") or app_row.get("ref") or ""),
        ("Full Name", app_row.get("full_name") or ""),
        ("Phone", app_row.get("phone") or app_row.get("contact_number") or ""),
        ("Email", app_row.get("email") or ""),
        ("Product", app_row.get("product_name") or ""),
        ("Amount", str(app_row.get("amount") or "")),
        ("Term", str(app_row.get("term") or "")),
        ("Status", app_row.get("status") or ""),
        ("Purpose", app_row.get("loan_purpose") or ""),
        ("Employment", app_row.get("employment_status") or ""),
        ("Address", app_row.get("physical_address") or ""),
        ("Town / City", app_row.get("town_city") or ""),
        ("Region", app_row.get("region") or ""),
        ("Review Note", app_row.get("review_note") or ""),
    ]

    c.setFont("Helvetica", 10)
    y -= 22
    for label, value in info:
        if y < 80:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)
        c.drawString(40, y, f"{label}: {value}")
        y -= 16

    y -= 10
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y, "Included Documents")
    y -= 18
    c.setFont("Helvetica", 10)

    if not docs:
        c.drawString(40, y, "No uploaded documents found.")
    else:
        for idx, d in enumerate(docs, start=1):
            if y < 80:
                c.showPage()
                y = height - 50
                c.setFont("Helvetica", 10)
            c.drawString(40, y, f"{idx}. {d.get('label')}")
            y -= 16

    c.save()
    buf.seek(0)
    return buf.getvalue()

def _append_file_to_writer(writer, file_bytes, label="Document"):
    # try pdf first
    try:
        pdf_reader = PdfReader(io.BytesIO(file_bytes))
        for page in pdf_reader.pages:
            writer.add_page(page)
        return True
    except Exception:
        pass

    # try image -> single-page pdf
    try:
        img = Image.open(io.BytesIO(file_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")
        temp_img_pdf = io.BytesIO()
        img.save(temp_img_pdf, format="PDF")
        temp_img_pdf.seek(0)
        pdf_reader = PdfReader(temp_img_pdf)
        for page in pdf_reader.pages:
            writer.add_page(page)
        return True
    except Exception:
        return False

@app.route("/admin/reports/applications.csv", methods=["GET"])
def admin_report_applications_csv():
    if not _require_admin_report():
        return redirect("/admin/login")

    rows = _rest_get_rows("applications", {
        "select": "*",
        "order": "created_at.desc"
    })

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([
        "reference", "full_name", "phone", "email", "product_name",
        "amount", "term", "status", "loan_purpose", "created_at"
    ])

    for r in rows:
        writer.writerow([
            r.get("reference") or r.get("ref") or "",
            r.get("full_name") or "",
            r.get("phone") or r.get("contact_number") or "",
            r.get("email") or "",
            r.get("product_name") or "",
            r.get("amount") or "",
            r.get("term") or "",
            r.get("status") or "",
            r.get("loan_purpose") or "",
            r.get("created_at") or "",
        ])

    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name="katramoney_applications_report.csv",
        mimetype="text/csv"
    )

@app.route("/admin/reports/applications.pdf", methods=["GET"])
def admin_report_applications_pdf():
    if not _require_admin_report():
        return redirect("/admin/login")

    rows = _rest_get_rows("applications", {
        "select": "*",
        "order": "created_at.desc"
    })

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    y = height - 40
    c.setFont("Helvetica-Bold", 18)
    c.drawString(40, y, "KATRAMONEY Applications Report")

    y -= 22
    c.setFont("Helvetica", 10)
    c.drawString(40, y, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    y -= 28
    c.setFont("Helvetica-Bold", 10)
    headers = ["Reference", "Applicant", "Phone", "Amount", "Status"]
    xs = [40, 160, 300, 410, 490]
    for x, h in zip(xs, headers):
        c.drawString(x, y, h)

    y -= 14
    c.line(40, y, 560, y)
    y -= 16
    c.setFont("Helvetica", 9)

    for r in rows:
        if y < 60:
            c.showPage()
            y = height - 40
            c.setFont("Helvetica", 9)
        c.drawString(40, y, str((r.get("reference") or r.get("ref") or ""))[:20])
        c.drawString(160, y, str(r.get("full_name") or "")[:24])
        c.drawString(300, y, str(r.get("phone") or r.get("contact_number") or "")[:16])
        c.drawString(410, y, "N$ " + str(r.get("amount") or ""))
        c.drawString(490, y, str(r.get("status") or "")[:12])
        y -= 16

    c.save()
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name="katramoney_applications_report.pdf",
        mimetype="application/pdf"
    )

@app.route("/admin/applications/<app_id>/profile-pdf", methods=["GET"])
def admin_application_profile_pdf(app_id):
    if not _require_admin_report():
        return redirect("/admin/login")

    app_row = _rest_get_one("applications", {
        "select": "*",
        "id": f"eq.{app_id}",
        "limit": "1"
    })

    if not app_row:
        return "Application not found.", 404

    docs = _guess_document_urls(app_row)

    writer = PdfWriter()

    cover_pdf = _make_cover_pdf_bytes(app_row, docs)
    cover_reader = PdfReader(io.BytesIO(cover_pdf))
    for page in cover_reader.pages:
        writer.add_page(page)

    for d in docs:
        try:
            fb = _download_file_bytes(d["url"])
            _append_file_to_writer(writer, fb, d.get("label") or "Document")
        except Exception:
            # skip broken file but keep the rest
            pass

    out = io.BytesIO()
    writer.write(out)
    out.seek(0)

    ref = app_row.get("reference") or app_row.get("ref") or app_id
    filename = f"{ref}_profile_pack.pdf"

    return send_file(
        out,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf"
    )

# ===== End Reports / Application profile PDF =====




@app.route("/admin/messages/<table_name>/<item_id>/delete", methods=["POST"])
def admin_message_delete_route(table_name, item_id):
    admin_email = session.get("admin_email") or session.get("email") or session.get("user_email")
    if not admin_email:
        return jsonify({"success": False, "error": "Admin login required."}), 401

    allowed = {"contacts", "customer_messages", "support_tickets"}
    if table_name not in allowed:
        return jsonify({"success": False, "error": "Invalid table."}), 400

    base_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
    if not base_url or not key:
        return jsonify({"success": False, "error": "Supabase config missing."}), 500

    import requests
    res = requests.delete(
        f"{base_url}/rest/v1/{table_name}",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        },
        params={"id": f"eq.{item_id}"},
        timeout=30
    )

    if res.status_code >= 400:
        return jsonify({"success": False, "error": "Failed to delete record.", "details": res.text[:500]}), 500

    return jsonify({"success": True, "message": "Record deleted successfully."})

@app.route("/admin/support/<ticket_id>/reply", methods=["POST"])
def admin_support_reply_route(ticket_id):
    admin_email = session.get("admin_email") or session.get("email") or session.get("user_email")
    if not admin_email:
        return jsonify({"success": False, "error": "Admin login required."}), 401

    payload = request.get_json(silent=True) or {}
    reply_message = str(payload.get("reply_message") or "").strip()
    status = str(payload.get("status") or "OPEN").strip()

    if not reply_message:
        return jsonify({"success": False, "error": "Reply message required."}), 400

    base_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
    if not base_url or not key:
        return jsonify({"success": False, "error": "Supabase config missing."}), 500

    import requests
    res = requests.patch(
        f"{base_url}/rest/v1/support_tickets",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        },
        params={"id": f"eq.{ticket_id}"},
        json={"admin_reply": reply_message, "status": status},
        timeout=30
    )

    if res.status_code >= 400:
        return jsonify({"success": False, "error": "Failed to save reply.", "details": res.text[:500]}), 500

    return jsonify({"success": True, "message": "Support reply saved."})



@app.route("/api/public-loan-products", methods=["GET"])
def api_public_loan_products():
    base_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = (
        os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or ""
    )

    if not base_url or not key:
        return jsonify({"success": False, "error": "Supabase config missing."}), 500

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json"
    }

    import requests
    res = requests.get(
        f"{base_url}/rest/v1/loan_products",
        headers=headers,
        params={
            "select": "*",
            "active": "eq.true",
            "order": "created_at.desc"
        },
        timeout=30
    )

    if res.status_code >= 400:
        return jsonify({
            "success": False,
            "error": "Failed to load public loan products.",
            "details": res.text[:500]
        }), 500

    try:
        rows = res.json()
    except Exception:
        rows = []

    if not isinstance(rows, list):
        rows = []

    cleaned = []
    for r in rows:
        cleaned.append({
            "id": r.get("id"),
            "name": r.get("name") or "Loan Product",
            "interest_rate": float(r.get("interest_rate") or 0),
            "service_fee": float(r.get("service_fee") or 0),
            "min_amount": float(r.get("min_amount") or 0),
            "max_amount": float(r.get("max_amount") or 0),
            "active": bool(r.get("active")),
            "terms": r.get("terms") or [1, 3, 6, 12],
            "description": r.get("description") or "Flexible financial support tailored for your needs."
        })

    return jsonify({"success": True, "data": cleaned})


if __name__ == "__main__":
    print_all_routes()
    app.run(debug=True, host="0.0.0.0", port=5000)


# =========================================================
# CUSTOMER PORTAL / APPLICATION SUBMIT / WAITING DASHBOARD
# =========================================================
import uuid
from datetime import datetime, timezone

def _safe_filename(name):
    name = (name or "").strip()
    name = name.replace("\\", "_").replace("/", "_").replace(":", "_")
    name = name.replace("*", "_").replace("?", "_").replace('"', "_")
    name = name.replace("<", "_").replace(">", "_").replace("|", "_")
    return name

def _ensure_upload_dir():
    upload_dir = os.path.join(BASE_DIR, "static", "uploads", "applications")
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir

def _save_upload(file_obj, prefix):
    try:
        if not file_obj or not getattr(file_obj, "filename", None):
            return ""
        filename = _safe_filename(file_obj.filename)
        if not filename:
            return ""
        ext = os.path.splitext(filename)[1]
        unique_name = f"{prefix}_{uuid.uuid4().hex}{ext}"
        upload_dir = _ensure_upload_dir()
        abs_path = os.path.join(upload_dir, unique_name)
        file_obj.save(abs_path)
        return f"/static/uploads/applications/{unique_name}"
    except Exception as e:
        print("FILE SAVE ERROR:", e)
        return ""


def _table_columns(table_name):
    try:
        # use a very small known-safe payload check strategy
        sample = supabase_admin.table(table_name).select("*").limit(1).execute()
        rows = getattr(sample, "data", None) or []
        if rows and isinstance(rows, list) and len(rows) > 0:
            return set(rows[0].keys())
    except Exception as e:
        print("TABLE COLUMN READ ERROR:", table_name, e)
    return set()

def _filter_payload_to_existing_columns(table_name, payload):
    cols = _table_columns(table_name)
    if cols:
        return {k: v for k, v in payload.items() if k in cols}

    # fallback allow-list for common applications columns when table is empty
    common_application_cols = {
        "reference","ref","full_name","phone","email","id_number","date_of_birth","gender",
        "physical_address","town_city","region","employment_status","employer_name",
        "monthly_income","other_income","amount","loan_amount","term","loan_term",
        "product_name","loan_purpose","next_of_kin_name","next_of_kin_phone",
        "next_of_kin_relationship","next_of_kin_address","geo_lat","geo_lng",
        "geo_accuracy","geo_timestamp","device_type","user_agent","platform_info",
        "screen_info","timezone_info","language_info","face_capture_data",
        "id_front_url","id_back_url","bank_statement_url","proof_of_income_url",
        "proof_of_address_url","supporting_doc_url","documents","status",
        "review_note","reply_message","created_at"
    }
    return {k: v for k, v in payload.items() if k in common_application_cols}
def _first_existing_table(*names):
    for name in names:
        try:
            supabase_admin.table(name).select("id").limit(1).execute()
            return name
        except Exception:
            continue
    return names[0] if names else None

def _load_customer_profile(email=None, phone=None):
    try:
        if email:
            rows = safe_rows("customer_profiles")
            for row in rows:
                if str(row.get("email") or "").strip().lower() == str(email).strip().lower():
                    return row
        if phone:
            rows = safe_rows("customer_profiles")
            for row in rows:
                if str(row.get("phone") or "").strip() == str(phone).strip():
                    return row
    except Exception as e:
        print("LOAD CUSTOMER PROFILE ERROR:", e)
    return {}

def _load_customer_applications(email=None, phone=None):
    table_name = _first_existing_table("applications", "loan_applications")
    try:
        rows = safe_rows(table_name, "created_at", True)
        filtered = []
        for row in rows:
            row_email = str(row.get("email") or "").strip().lower()
            row_phone = str(row.get("phone") or row.get("contact_number") or "").strip()
            if email and row_email == str(email).strip().lower():
                filtered.append(row)
            elif phone and row_phone == str(phone).strip():
                filtered.append(row)
        return table_name, filtered
    except Exception as e:
        print("LOAD CUSTOMER APPLICATIONS ERROR:", e)
        return table_name, []

def _build_status_history(applications):
    history = []
    for app in applications:
        history.append({
            "status": app.get("status") or "PENDING",
            "note": app.get("review_note") or app.get("reply_message") or "Application received and waiting for admin review.",
            "created_at": app.get("created_at") or ""
        })
    history.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return history

def _customer_messages_for(email=None, phone=None):
    rows = safe_rows("customer_messages", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        row_customer_id = str(row.get("customer_id") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
        elif email and row_customer_id == str(email).strip().lower():
            out.append(row)
    return out

def _support_tickets_for(email=None, phone=None):
    rows = safe_rows("support_tickets", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("customer_email") or row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out

def _loan_accounts_for(email=None, phone=None):
    rows = safe_rows("loan_accounts", "opened_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out

def _loan_payments_for(email=None, phone=None):
    rows = safe_rows("loan_payments", "created_at", True)
    out = []
    for row in rows:
        row_email = str(row.get("email") or "").strip().lower()
        row_phone = str(row.get("phone") or "").strip()
        if email and row_email == str(email).strip().lower():
            out.append(row)
        elif phone and row_phone == str(phone).strip():
            out.append(row)
    return out



# duplicate removed











