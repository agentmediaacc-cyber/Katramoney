import os
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, flash
from supabase import create_client, Client

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"))
app.secret_key = (os.getenv("FLASK_SECRET_KEY") or "katramoney_admin_2026").strip()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().strip('"').rstrip("/")
SUPABASE_ANON_KEY = (os.getenv("SUPABASE_ANON_KEY") or "").strip().strip('"')
SUPABASE_SERVICE_KEY = (
    os.getenv("SUPABASE_SERVICE_KEY")
    or os.getenv("SUPABASE_SERVICE_ROLE")
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or ""
).strip().strip('"')

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is missing in .env")
if not SUPABASE_ANON_KEY:
    raise RuntimeError("SUPABASE_ANON_KEY is missing in .env")
if not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_KEY is missing in .env")

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
    rows = safe_rows("applications", "created_at", True)
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

        update_payload = {
            "reply_message": to_str(body.get("reply_message")),
            "status": to_str(body.get("status") or "OPEN")
        }

        safe_payload = _filter_payload_to_existing_columns("support_tickets", update_payload)
        result = supabase_admin.table("support_tickets").update(safe_payload).eq("id", ticket_id).execute()

        if wants_json():
            return json_ok("Support reply saved.", rows_from(result))

        flash("Support reply saved.", "success")
        return redirect("/admin#messages")
    except Exception as e:
        print("SUPPORT REPLY ERROR:", e)
        if wants_json():
            return json_error(e, 500)
        flash(f"Failed to save support reply: {e}", "error")
        return redirect("/admin#messages")

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











