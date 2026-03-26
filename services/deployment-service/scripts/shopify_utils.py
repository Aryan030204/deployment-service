import requests
import json
from datetime import datetime, timedelta

def get_dynamic_app_id_mapping(shop_name: str, access_token: str, brand_name: str) -> str:
    """
    Queries Shopify GraphQL API for the last 5 days to extract unique App IDs and Names.
    """
    print(f"🔍 Generating dynamic App ID mapping for {shop_name}...")
    
    endpoint = f"https://{shop_name}.myshopify.com/admin/api/2026-01/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    
    app_mapping = {}
    
    # 1. Iterate through last 5 days
    for i in range(1, 6):
        query_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        
        query = {
            "query": f'query {{ orders(first: 250, query: "created_at:>={query_date}") {{ edges {{ node {{ app {{ id name }} }} }} }} }}'
        }
        
        try:
            response = requests.post(endpoint, json=query, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                edges = data.get("data", {}).get("orders", {}).get("edges", [])
                
                for edge in edges:
                    app = edge.get("node", {}).get("app")
                    if app:
                        gid = app.get("id")
                        raw_name = app.get("name")
                        
                        if gid and raw_name:
                            # Extract ID slug: gid://shopify/App/269375733761 -> 269375733761
                            app_id = gid.split("/")[-1]
                            
                            # Clean name: "Gokwik <> Bla Bli Blu" -> "Gokwik"
                            name = raw_name
                            if " <> " in raw_name:
                                # Split by <> and strip. If brand_name matches one side, take the other.
                                parts = [p.strip() for p in raw_name.split("<>")]
                                if len(parts) > 1:
                                    # If brand_name is part of the string, remove it.
                                    # Otherwise, take the first part as the app name.
                                    if parts[1].lower() == brand_name.lower():
                                        name = parts[0]
                                    elif parts[0].lower() == brand_name.lower():
                                        name = parts[1]
                                    else:
                                        name = parts[0] # Default to first part
                            
                            app_mapping[app_id] = name
            else:
                print(f"⚠️ Shopify GraphQL failed for {query_date}: {response.status_code}")
        except Exception as e:
            print(f"❌ Error fetching Shopify orders for {query_date}: {e}")

    # Fallback to defaults if mapping is empty
    if not app_mapping:
        print("⚠️ Dynamic mapping empty, using defaults.")
        app_mapping = {
            "238372913153": "GoKwik",
            "580111": "Online Store",
            "1354745": "Draft Order"
        }
    
    result = json.dumps(app_mapping)
    print(f"✅ Generated Mapping: {result}")
    return result
