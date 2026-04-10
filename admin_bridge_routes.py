import uuid
from datetime import datetime, timedelta
from flask import request, jsonify

def register_admin_bridge_routes(app, supabase):
    def to_int(value, default=0):
        try:
            if value is None or value == "":
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    def to_float(value, default=0):
        try:
            if value is None or value == "":
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def create_repayment_schedule(loan_account_id, total_repayment, months):
        months = max(1, to_int(months, 1))
        total_repayment = to_int(total_repayment, 0)

        monthly_amount = total_repayment // months
        remainder = total_repayment - (monthly_amount * months)

        rows = []
        today = datetime.utcnow().date()

        for i in range(1, months + 1):
            amount = monthly_amount + (remainder if i == months else 0)
            due_date = today + timedelta(days=30 * i)
            rows.append({
                "loan_account_id": loan_account_id,
                "installment_no": i,
                "due_date": due_date.isoformat(),
                "due_amount": amount,
                "paid_amount": 0,
                "status": "UNPAID"
            })

        if rows:
            supabase.table("loan_repayment_schedule").insert(rows).execute()

    @app.route("/api/admin/application-decision/<app_id>", methods=["POST"])
    def admin_application_decision(app_id):
        data = request.json or {}

        try:
            app_res = supabase.table("applications").select("*").eq("id", app_id).limit(1).execute()
            application = app_res.data[0] if app_res and app_res.data else None

            if not application:
                return jsonify({"success": False, "error": "Application not found"}), 404

            status = (data.get("status") or "PENDING").strip().upper()
            admin_note = (data.get("admin_note") or "").strip()
            customer_message = (data.get("customer_message") or "").strip()
            approved_amount = to_int(data.get("approved_amount"), application.get("amount", 0))
            interest_rate = to_float(data.get("interest_rate"), 0)
            service_fee = to_int(data.get("service_fee"), 0)
            decision_by = "ADMIN"

            update_payload = {
                "status": status,
                "current_stage": status,
                "admin_note": admin_note,
                "approved_amount": approved_amount,
                "approved_interest_rate": interest_rate,
                "approved_service_fee": service_fee,
                "decision_at": datetime.utcnow().isoformat(),
                "decision_by": decision_by
            }

            supabase.table("applications").update(update_payload).eq("id", app_id).execute()

            supabase.table("application_status_history").insert({
                "application_id": app_id,
                "status": status,
                "note": admin_note or f"Application moved to {status}",
                "changed_by": decision_by
            }).execute()

            customer_id = application.get("customer_id")
            if customer_id:
                msg_text = customer_message or admin_note or f"Your application status is now {status}."
                supabase.table("customer_messages").insert({
                    "customer_id": customer_id,
                    "subject": f"Application {status}",
                    "message": msg_text,
                    "sender_role": "ADMIN",
                    "is_read": False
                }).execute()

            if status == "APPROVED" and customer_id:
                existing_loan = supabase.table("loan_accounts").select("*").eq("application_id", app_id).limit(1).execute()
                loan = existing_loan.data[0] if existing_loan and existing_loan.data else None

                total_repayment = int(round(approved_amount + (approved_amount * (interest_rate / 100.0)) + service_fee))
                balance_amount = total_repayment
                account_number = f"KATRA-{uuid.uuid4().hex[:10].upper()}"

                if loan:
                    loan_id = loan["id"]
                    supabase.table("loan_accounts").update({
                        "principal_amount": approved_amount,
                        "approved_amount": approved_amount,
                        "interest_rate": interest_rate,
                        "service_fee": service_fee,
                        "total_repayment": total_repayment,
                        "paid_amount": loan.get("paid_amount", 0) or 0,
                        "balance_amount": balance_amount,
                        "status": "ACTIVE",
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", loan_id).execute()

                    supabase.table("loan_repayment_schedule").delete().eq("loan_account_id", loan_id).execute()
                    create_repayment_schedule(loan_id, total_repayment, application.get("term", 1))
                else:
                    loan_res = supabase.table("loan_accounts").insert({
                        "customer_id": customer_id,
                        "application_id": app_id,
                        "account_number": account_number,
                        "principal_amount": approved_amount,
                        "approved_amount": approved_amount,
                        "interest_rate": interest_rate,
                        "service_fee": service_fee,
                        "total_repayment": total_repayment,
                        "paid_amount": 0,
                        "balance_amount": balance_amount,
                        "status": "ACTIVE"
                    }).execute()

                    loan_id = loan_res.data[0]["id"]
                    create_repayment_schedule(loan_id, total_repayment, application.get("term", 1))

            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/admin/customer-message", methods=["POST"])
    def admin_customer_message():
        data = request.json or {}
        try:
            customer_id = data.get("customer_id")
            subject = (data.get("subject") or "Admin Message").strip()
            message = (data.get("message") or "").strip()

            if not customer_id or not message:
                return jsonify({"success": False, "error": "customer_id and message are required"}), 400

            supabase.table("customer_messages").insert({
                "customer_id": customer_id,
                "subject": subject,
                "message": message,
                "sender_role": "ADMIN",
                "is_read": False
            }).execute()

            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/admin/payment-proof/<payment_id>/review", methods=["POST"])
    def admin_payment_proof_review(payment_id):
        data = request.json or {}
        try:
            review_status = (data.get("review_status") or "APPROVED").strip().upper()
            note = (data.get("note") or "").strip()

            pay_res = supabase.table("loan_payments").select("*").eq("id", payment_id).limit(1).execute()
            payment = pay_res.data[0] if pay_res and pay_res.data else None

            if not payment:
                return jsonify({"success": False, "error": "Payment proof not found"}), 404

            supabase.table("loan_payments").update({
                "review_status": review_status,
                "payment_note": note or payment.get("payment_note")
            }).eq("id", payment_id).execute()

            if review_status == "APPROVED" and payment.get("loan_account_id"):
                loan_res = supabase.table("loan_accounts").select("*").eq("id", payment["loan_account_id"]).limit(1).execute()
                loan = loan_res.data[0] if loan_res and loan_res.data else None

                if loan:
                    payment_amount = to_int(payment.get("payment_amount"), 0)
                    new_paid = to_int(loan.get("paid_amount"), 0) + payment_amount
                    total_repayment = to_int(loan.get("total_repayment"), 0)
                    new_balance = max(0, total_repayment - new_paid)

                    loan_status = "ACTIVE"
                    if new_balance == 0:
                        loan_status = "PAID"

                    supabase.table("loan_accounts").update({
                        "paid_amount": new_paid,
                        "balance_amount": new_balance,
                        "status": loan_status,
                        "updated_at": datetime.utcnow().isoformat()
                    }).eq("id", loan["id"]).execute()

            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
