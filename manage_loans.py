import os
from dotenv import load_dotenv
from supabase import create_client, Client

# This line is the fix - it loads your .env file
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_ANON_KEY")

if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_ANON_KEY not found in .env file.")
    exit()

supabase: Client = create_client(url, key)

def manage_applications():
    print("\n--- KATRAMONEY LOAN MANAGER ---")
    res = supabase.table("applications").select("*").eq("status", "pending").execute()
    apps = res.data

    if not apps:
        print("No pending applications found.")
        return

    print(f"{'ID':<4} | {'NAME':<20} | {'AMOUNT':<10}")
    print("-" * 40)
    
    for idx, app in enumerate(apps):
        print(f"{idx:<4} | {app['full_name']:<20} | N$ {app['amount']}")

    try:
        user_input = input("\nEnter the ID number to update (or 'q' to exit): ")
        if user_input.lower() == 'q': return

        choice = int(user_input)
        selected_app = apps[choice]
        new_status = input(f"Update {selected_app['full_name']} to (A)pproved or (R)ejected? ").lower()

        status_map = {'a': 'approved', 'r': 'rejected'}
        if new_status in status_map:
            final_status = status_map[new_status]
            supabase.table("applications").update({"status": final_status}).eq("id", selected_app["id"]).execute()
            print(f"Successfully updated to {final_status.upper()}.")
        else:
            print("Invalid selection.")
            
    except (ValueError, IndexError):
        print("Invalid input. Please enter a valid ID from the list.")

if __name__ == "__main__":
    manage_applications()
