import os
import uuid
from datetime import datetime
from flask import render_template, request, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename


def register_customer_portal_routes(app, supabase):
    upload_root = os.path.join(app.root_path, "uploads")
    os.makedirs(upload_root, exist_ok=True)

    def to_int(value, default=0):
        try:
            if value is None or value == "":
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    def save_upload(file_obj, folder_name):
        if not file_obj or not file_obj.filename:
            return None, None

        folder = os.path.join(upload_root, folder_name)
        os.makedirs(folder, exist_ok=True)

        original = secure_filename(file_obj.filename)
        filename = f"{uuid.uuid4().hex}_{original}"
        full_path = os.path.join(folder, filename)
        file_obj.save(full_path)

        return f"{folder_name}/{filename}", original

    def get_site_config():
        try:
            conf_res = supabase.table("site_config").select("*").limit(1).execute()
            return conf_res.data[0] if conf_res and conf_res.data else {}
        except Exception:
            return {}

    @app.route("/customer/signup", methods=["GET", "POST"])
    def customer_signup():
        error = None
        next_url = request.args.get("next") or "/customer/dashboard"

        if request.method == "POST":
            full_name = (request.form.get("full_name") or "").strip()
            email = (request.form.get("email") or "").strip().lower()
            phone = (request.form.get("phone") or "").strip()
            password = request.form.get("password") or ""
            confirm_password = request.form.get("confirm_password") or ""

            if not full_name or not email or not phone or not password:
                error = "Please complete all required signup fields."
            elif password != confirm_password:
                error = "Passwords do not match."
            else:
                try:
                    existing = supabase.table("customer_accounts").select("id").eq("email", email).limit(1).execute()
                    if existing and existing.data:
                        error = "This email already has an account."
                    else:
                        acct = supabase.table("customer_accounts").insert({
                            "email": email,
                            "password_hash": generate_password_hash(password),
                            "full_name": full_name,
                            "phone": phone,
                            "is_active": True
                        }).execute()

                        customer = acct.data[0]

                        supabase.table("customer_profiles").insert({
                            "customer_id": customer["id"],
                            "full_name": full_name,
                            "phone": phone
                        }).execute()

                        session["customer_logged_in"] = True
                        session["customer_account_id"] = customer["id"]
                        session["customer_email"] = email

                        return redirect(next_url)
                except Exception as e:
                    error = f"Signup failed: {str(e)}"

        return render_template("customer_signup.html", error=error, next_url=next_url)

    @app.route("/customer/login", methods=["GET", "POST"])
    def customer_login():
        error = None
        next_url = request.args.get("next") or "/customer/dashboard"

        if request.method == "POST":
            email = (request.form.get("email") or "").strip().lower()
            password = request.form.get("password") or ""

            try:
                res = supabase.table("customer_accounts").select("*").eq("email", email).limit(1).execute()
                customer = res.data[0] if res and res.data else None

                if not customer:
                    error = "Account not found."
                elif not customer.get("is_active", True):
                    error = "This account is disabled."
                elif not check_password_hash(customer.get("password_hash", ""), password):
                    error = "Invalid email or password."
                else:
                    session["customer_logged_in"] = True
                    session["customer_account_id"] = customer["id"]
                    session["customer_email"] = customer["email"]
                    return redirect(next_url)
            except Exception as e:
                error = f"Login failed: {str(e)}"

        return render_template("customer_login.html", error=error, next_url=next_url)

    @app.route("/customer/logout")
    def customer_logout():
        session.pop("customer_logged_in", None)
        session.pop("customer_account_id", None)
        session.pop("customer_email", None)
        return redirect(url_for("customer_login"))

    @app.route("/apply")
    def apply_page():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_signup", next="/apply"))

        customer_id = session.get("customer_account_id")
        profile = {}

        try:
            res = supabase.table("customer_profiles").select("*").eq("customer_id", customer_id).limit(1).execute()
            profile = res.data[0] if res and res.data else {}
        except Exception:
            profile = {}

        return render_template(
            "apply.html",
            profile=profile,
            customer_email=session.get("customer_email")
        )

    @app.route("/customer/secure-apply", methods=["POST"])
    def customer_secure_apply():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_login", next="/apply"))

        customer_id = session.get("customer_account_id")
        customer_email = session.get("customer_email")

        try:
            full_name = (request.form.get("full_name") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            email = (request.form.get("email") or customer_email or "").strip()
            id_number = (request.form.get("id_number") or "").strip()
            date_of_birth = (request.form.get("date_of_birth") or "").strip()
            gender = (request.form.get("gender") or "").strip()
            physical_address = (request.form.get("physical_address") or "").strip()
            town_city = (request.form.get("town_city") or "").strip()
            region = (request.form.get("region") or "").strip()
            employment_status = (request.form.get("employment_status") or "").strip()
            employer_name = (request.form.get("employer_name") or "").strip()
            monthly_income = to_int(request.form.get("monthly_income"), 0)
            other_income = to_int(request.form.get("other_income"), 0)
            loan_amount = to_int(request.form.get("loan_amount"), 0)
            loan_term = to_int(request.form.get("loan_term"), 1)
            loan_purpose = (request.form.get("loan_purpose") or "").strip()
            next_of_kin_name = (request.form.get("next_of_kin_name") or "").strip()
            next_of_kin_phone = (request.form.get("next_of_kin_phone") or "").strip()
            next_of_kin_relationship = (request.form.get("next_of_kin_relationship") or "").strip()
            next_of_kin_address = (request.form.get("next_of_kin_address") or "").strip()
            geo_lat = request.form.get("geo_lat")
            geo_lng = request.form.get("geo_lng")
            geo_accuracy = request.form.get("geo_accuracy")
            geo_timestamp = request.form.get("geo_timestamp")
            device_type = request.form.get("device_type")
            user_agent = request.form.get("user_agent")
            platform_info = request.form.get("platform_info")
            screen_info = request.form.get("screen_info")
            timezone_info = request.form.get("timezone_info")
            language_info = request.form.get("language_info")
            face_capture_data = request.form.get("face_capture_data")
            agreed = request.form.get("agree_terms")

            if not full_name or not phone or not id_number or loan_amount <= 0:
                return "Missing required application data.", 400

            if not agreed:
                return "You must accept the terms and conditions.", 400

            profile_payload = {
                "full_name": full_name,
                "phone": phone,
                "id_number": id_number,
                "date_of_birth": date_of_birth if date_of_birth else None,
                "gender": gender,
                "physical_address": physical_address,
                "town_city": town_city,
                "region": region,
                "next_of_kin_name": next_of_kin_name,
                "next_of_kin_phone": next_of_kin_phone,
                "next_of_kin_relationship": next_of_kin_relationship,
                "next_of_kin_address": next_of_kin_address,
                "employment_status": employment_status,
                "employer_name": employer_name,
                "monthly_income": monthly_income,
                "other_income": other_income,
                "updated_at": datetime.utcnow().isoformat()
            }

            existing_profile = supabase.table("customer_profiles").select("id").eq("customer_id", customer_id).limit(1).execute()
            if existing_profile and existing_profile.data:
                supabase.table("customer_profiles").update(profile_payload).eq("customer_id", customer_id).execute()
            else:
                profile_payload["customer_id"] = customer_id
                supabase.table("customer_profiles").insert(profile_payload).execute()

            uploaded_docs = []
            doc_map = [
                ("id_front", "ID_FRONT"),
                ("id_back", "ID_BACK"),
                ("bank_statement", "BANK_STATEMENT"),
                ("proof_of_income", "PROOF_OF_INCOME"),
                ("proof_of_address", "PROOF_OF_ADDRESS"),
                ("supporting_doc", "SUPPORTING_DOC")
            ]

            for field_name, _doc_type in doc_map:
                f = request.files.get(field_name)
                saved_path, original_name = save_upload(f, "applications")
                if saved_path:
                    uploaded_docs.append({
                        "field": field_name,
                        "filename": saved_path,
                        "original_name": original_name
                    })

            payload = {
                "reference": f"KATRA-{uuid.uuid4().hex[:8].upper()}",
                "customer_id": customer_id,
                "full_name": full_name,
                "phone": phone,
                "email": email,
                "id_number": id_number,
                "date_of_birth": date_of_birth,
                "gender": gender,
                "physical_address": physical_address,
                "town_city": town_city,
                "region": region,
                "employment_status": employment_status,
                "employer_name": employer_name,
                "monthly_income": monthly_income,
                "other_income": other_income,
                "amount": loan_amount,
                "term": loan_term,
                "loan_purpose": loan_purpose,
                "next_of_kin_name": next_of_kin_name,
                "next_of_kin_phone": next_of_kin_phone,
                "next_of_kin_relationship": next_of_kin_relationship,
                "next_of_kin_address": next_of_kin_address,
                "status": "PENDING",
                "current_stage": "SUBMITTED",
                "admin_note": "New customer application submitted",
                "geo_lat": float(geo_lat) if geo_lat else None,
                "geo_lng": float(geo_lng) if geo_lng else None,
                "geo_accuracy": float(geo_accuracy) if geo_accuracy else None,
                "geo_timestamp": geo_timestamp if geo_timestamp else None,
                "device_type": device_type,
                "user_agent": user_agent,
                "platform_info": platform_info,
                "screen_info": screen_info,
                "timezone_info": timezone_info,
                "language_info": language_info,
                "face_capture_data": face_capture_data,
                "uploaded_docs": uploaded_docs
            }

            app_res = supabase.table("applications").insert(payload).execute()
            application = app_res.data[0]

            supabase.table("application_status_history").insert({
                "application_id": application["id"],
                "status": "SUBMITTED",
                "note": "Application submitted by customer",
                "changed_by": "CUSTOMER"
            }).execute()

            for doc in uploaded_docs:
                supabase.table("application_documents").insert({
                    "application_id": application["id"],
                    "customer_id": customer_id,
                    "document_type": doc["field"],
                    "file_path": doc["filename"],
                    "original_name": doc["original_name"],
                    "review_status": "PENDING"
                }).execute()

            if face_capture_data:
                face_folder = os.path.join(upload_root, "face_capture")
                os.makedirs(face_folder, exist_ok=True)
                face_filename = f"face_{uuid.uuid4().hex}.txt"
                with open(os.path.join(face_folder, face_filename), "w", encoding="utf-8") as fh:
                    fh.write(face_capture_data)

            return redirect(url_for("customer_dashboard"))

        except Exception as e:
            return f"Secure application submission failed: {str(e)}", 500

    

    @app.route("/customer/support/new", methods=["POST"])
    
    @app.route("/customer/profile/save", methods=["POST"])
    def customer_profile_save():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_login"))

        customer_id = session.get("customer_account_id")

        try:
            payload = {
                "full_name": request.form.get("full_name"),
                "phone": request.form.get("phone"),
                "physical_address": request.form.get("physical_address"),
                "town_city": request.form.get("town_city"),
                "region": request.form.get("region"),
                "updated_at": datetime.utcnow().isoformat()
            }

            existing = supabase.table("customer_profiles").select("id").eq("customer_id", customer_id).limit(1).execute()

            if existing and existing.data:
                supabase.table("customer_profiles").update(payload).eq("customer_id", customer_id).execute()
            else:
                payload["customer_id"] = customer_id
                supabase.table("customer_profiles").insert(payload).execute()

            return redirect(url_for("customer_dashboard"))

        except Exception as e:
            return f"Profile save failed: {str(e)}", 500

    def customer_support_new():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_login"))

        customer_id = session.get("customer_account_id")

        try:
            supabase.table("support_tickets").insert({
                "customer_id": customer_id,
                "subject": request.form.get("subject"),
                "message": request.form.get("message"),
                "status": "OPEN",
                "priority": "NORMAL"
            }).execute()
            return redirect(url_for("customer_dashboard"))
        except Exception as e:
            return f"Support ticket failed: {str(e)}", 500

    @app.route("/customer/payment-proof/upload", methods=["POST"])
    def customer_payment_proof_upload():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_login"))

        customer_id = session.get("customer_account_id")

        try:
            loan_account_id = request.form.get("loan_account_id")
            payment_amount = to_int(request.form.get("payment_amount"), 0)
            payment_method = request.form.get("payment_method")
            payment_note = request.form.get("payment_note")
            proof = request.files.get("payment_proof")

            saved_path, original_name = save_upload(proof, "payment_proofs")

            supabase.table("loan_payments").insert({
                "loan_account_id": loan_account_id,
                "customer_id": customer_id,
                "payment_amount": payment_amount,
                "payment_method": payment_method,
                "proof_file_path": saved_path,
                "original_name": original_name,
                "payment_note": payment_note,
                "review_status": "PENDING"
            }).execute()

            return redirect(url_for("customer_dashboard"))
        except Exception as e:
            return f"Payment proof upload failed: {str(e)}", 500

    @app.route("/customer/dashboard")
    def customer_dashboard():
        if not session.get("customer_logged_in"):
            return redirect(url_for("customer_login"))

        customer_id = session.get("customer_account_id")
        customer_email = session.get("customer_email")
        config = get_site_config()

        profile = {}
        applications = []
        latest_application = None
        status_history = []
        loan_accounts = []
        repayment_schedule = []
        customer_messages = []
        support_tickets = []
        approved_amount = 0
        paid_amount = 0
        balance_amount = 0
        current_status = "NO APPLICATION"

        try:
            profile_res = supabase.table("customer_profiles").select("*").eq("customer_id", customer_id).limit(1).execute()
            profile = profile_res.data[0] if profile_res and profile_res.data else {}

            app_res = supabase.table("applications").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
            applications = app_res.data if app_res and app_res.data else []
            latest_application = applications[0] if applications else None

            if latest_application:
                current_status = (latest_application.get("status") or "PENDING").upper()
                hist_res = supabase.table("application_status_history").select("*").eq("application_id", latest_application["id"]).order("created_at", desc=False).execute()
                status_history = hist_res.data if hist_res and hist_res.data else []

            loan_res = supabase.table("loan_accounts").select("*").eq("customer_id", customer_id).order("opened_at", desc=True).execute()
            loan_accounts = loan_res.data if loan_res and loan_res.data else []

            if loan_accounts:
                loan = loan_accounts[0]
                approved_amount = to_int(loan.get("approved_amount"), 0)
                paid_amount = to_int(loan.get("paid_amount"), 0)
                balance_amount = to_int(loan.get("balance_amount"), 0)

                sched_res = supabase.table("loan_repayment_schedule").select("*").eq("loan_account_id", loan["id"]).order("installment_no", desc=False).execute()
                repayment_schedule = sched_res.data if sched_res and sched_res.data else []

            msg_res = supabase.table("customer_messages").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
            customer_messages = msg_res.data if msg_res and msg_res.data else []

            ticket_res = supabase.table("support_tickets").select("*").eq("customer_id", customer_id).order("created_at", desc=True).execute()
            support_tickets = ticket_res.data if ticket_res and ticket_res.data else []

            return render_template(
                "customer_dashboard.html",
                customer_email=customer_email,
                profile=profile,
                applications=applications,
                latest_application=latest_application,
                status_history=status_history,
                loan_accounts=loan_accounts,
                repayment_schedule=repayment_schedule,
                customer_messages=customer_messages,
                support_tickets=support_tickets,
                approved_amount=approved_amount,
                paid_amount=paid_amount,
                balance_amount=balance_amount,
                current_status=current_status,
                customer_wallpaper_url=config.get("customer_wallpaper_url") or "",
                powered_by_text=config.get("powered_by_text") or "Powered by CyD",
                developer_credit=config.get("developer_credit") or "Developed by Tjandja Kasera",
                dashboard_ad_headline=config.get("dashboard_ad_headline") or "Luxury finance experience",
                dashboard_ad_text=config.get("dashboard_ad_text") or "Your portal is live, secure, and designed to feel premium."
            )

        except Exception as e:
            return f"Customer dashboard error: {str(e)}", 500

