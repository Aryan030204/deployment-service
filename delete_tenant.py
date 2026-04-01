import os
import sys
from dotenv import load_dotenv

# Load .env
load_dotenv(".env")

# Setup sys.path to include services/deployment-service
sys.path.append(os.path.join(os.getcwd(), "services", "deployment-service"))

# Now we can import the logic from the scripts folder
try:
    from scripts.delete_tenant_logic import delete_tenant_logic
except ImportError as e:
    print(f"❌ Error during import: {e}")
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            brand_id = int(sys.argv[1])
            success, msg = delete_tenant_logic(brand_id)
            if not success:
                print(f"❌ Error: {msg}")
                sys.exit(1)
            else:
                print("✅ Deletion process completed successfully.")
        except ValueError:
            print("❌ Error: Invalid brand ID. Must be an integer.")
            sys.exit(1)
    else:
        print("Usage: python delete_tenant.py <brand_id>")
