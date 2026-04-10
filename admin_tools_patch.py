from flask import jsonify, request

def register_admin_tools_patch(app, supabase):
    def _rows(table_name, order_by="created_at", desc=True):
        try:
            q = supabase.table(table_name).select("*")
            if order_by:
                q = q.order(order_by, desc=desc)
            res = q.execute()
            return getattr(res, "data", []) or []
        except Exception as e:
            print(f"[PATCH FETCH ERROR] {table_name}: {e}")
            return []

    def _one(table_name):
        rows = _rows(table_name, order_by=None, desc=True)
        return rows[0] if rows else {}

    def _json_ok(message="OK", data=None):
        return jsonify({"success": True, "message": message, "data": data if data is not None else []})

    def _json_error(message="Failed", status=500):
        return jsonify({"success": False, "error": str(message)}), status

    @app.route("/api/admin/overview", methods=["GET"])
    def patch_admin_overview():
        try:
            apps = _rows("applications")
            loan_products = _rows("loan_products")
            contacts = _rows("contacts")
            customer_messages = _rows("customer_messages")
            support_tickets = _rows("support_tickets")

            pending = len([a for a in apps if (a.get("status") or "PENDING") == "PENDING"])
            total_val = sum(float(a.get("amount") or 0) for a in apps)
            active_products = len([p for p in loan_products if bool(p.get("active"))])

            recent_apps = apps[:8]

            return jsonify({
                "success": True,
                "counts": {
                    "applications": len(apps),
                    "pending": pending,
                    "loan_products": len(loan_products),
                    "active_products": active_products,
                    "contacts": len(contacts),
                    "customer_messages": len(customer_messages),
                    "support_tickets": len(support_tickets),
                    "total_requested": total_val
                },
                "recent_applications": recent_apps
            })
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/api/admin/applications", methods=["GET"])
    def patch_admin_applications():
        try:
            return jsonify({"success": True, "data": _rows("applications")})
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/api/admin/loan-products", methods=["GET"])
    def patch_admin_loan_products():
        try:
            return jsonify({"success": True, "data": _rows("loan_products", order_by="id", desc=False)})
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/api/admin/messages-feed", methods=["GET"])
    def patch_admin_messages_feed():
        try:
            return jsonify({
                "success": True,
                "contacts": _rows("contacts"),
                "customer_messages": _rows("customer_messages"),
                "support_tickets": _rows("support_tickets")
            })
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/api/admin/site-settings", methods=["GET"])
    def patch_admin_site_settings_get():
        try:
            return jsonify({"success": True, "data": _one("site_settings")})
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/admin/site-settings/apply-products", methods=["POST"])
    def patch_apply_site_settings_to_products():
        try:
            settings = _one("site_settings")
            if not settings:
                return _json_error("No site_settings row found.", 400)

            products = _rows("loan_products", order_by="id", desc=False)
            updated = []

            personal_rate = float(settings.get("personal_loan_rate") or settings.get("interest_rate") or 0)
            business_rate = float(settings.get("business_loan_rate") or settings.get("interest_rate") or 0)
            salary_rate = float(settings.get("salary_advance_rate") or settings.get("interest_rate") or 0)
            service_fee = float(settings.get("service_fee") or 0)

            for p in products:
                name = str(p.get("name") or "").lower()

                rate_to_use = personal_rate
                if "business" in name:
                    rate_to_use = business_rate
                elif "salary" in name:
                    rate_to_use = salary_rate
                elif "advance" in name:
                    rate_to_use = salary_rate
                else:
                    rate_to_use = personal_rate

                payload = {
                    "interest_rate": rate_to_use,
                    "service_fee": service_fee
                }

                supabase.table("loan_products").update(payload).eq("id", p["id"]).execute()
                updated.append({"id": p["id"], "name": p.get("name"), "interest_rate": rate_to_use, "service_fee": service_fee})

            return _json_ok("Site settings applied to loan products.", updated)
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/admin/messages/reply", methods=["POST"])
    def patch_admin_message_reply():
        try:
            payload = request.get_json(silent=True) or request.form.to_dict() or {}

            customer_id = payload.get("customer_id")
            subject = (payload.get("subject") or "Admin Reply").strip()
            message = (payload.get("message") or "").strip()

            if not message:
                return _json_error("Reply message is required.", 400)

            record = {
                "customer_id": customer_id,
                "subject": subject,
                "message": message,
                "sender_role": "admin",
                "is_read": False
            }

            clean = {k: v for k, v in record.items() if v not in [None, ""]}

            try:
                result = supabase.table("customer_messages").insert(clean).execute()
            except Exception:
                fallback = {
                    "subject": subject,
                    "message": message,
                    "sender_role": "admin",
                    "is_read": False
                }
                result = supabase.table("customer_messages").insert(fallback).execute()

            return _json_ok("Reply sent and saved for customer portal.", getattr(result, "data", []))
        except Exception as e:
            return _json_error(e, 500)

    @app.route("/admin/messages/<table_name>/<row_id>/delete", methods=["POST"])
    def patch_admin_message_delete(table_name, row_id):
        try:
            allowed = {"contacts", "customer_messages", "support_tickets"}
            if table_name not in allowed:
                return _json_error("Delete not allowed for this table.", 400)

            result = supabase.table(table_name).delete().eq("id", row_id).execute()
            return _json_ok(f"Deleted from {table_name}.", getattr(result, "data", []))
        except Exception as e:
            return _json_error(e, 500)
