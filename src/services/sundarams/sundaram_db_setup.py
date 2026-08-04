import os
import sys
import yaml
import psycopg2
import re
import pandas as pd

def load_db_config():
    # Attempt to locate config/config.yaml from backend root
    # We check the current working directory first, and step up if needed
    paths = ['config/config.yaml', '../config/config.yaml', '../../config/config.yaml']
    for path in paths:
        if os.path.exists(path):
            print(f"Loading config from: {os.path.abspath(path)}")
            try:
                with open(path, 'r') as f:
                    config = yaml.safe_load(f)
                    return config.get('postgres', {})
            except Exception as e:
                print(f"Error loading config from {path}: {e}")
    print("Could not locate config/config.yaml. Please ensure you are running this from the backend root.")
    return None

def parse_vendor_master_file(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return []
        
    print(f"Parsing raw text from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    pattern = r'"([^"]+)":\s*\{([^}]+)\}'
    matches = re.findall(pattern, content, re.DOTALL)
    
    vendors = []
    for name, body in matches:
        name = name.strip()
        if name.startswith('#') or name.startswith('//'):
            continue
            
        entity_match = re.search(r'"entity":\s*"([^"]*)"', body)
        dept_match = re.search(r'"department":\s*"([^"]*)"', body)
        loc_match = re.search(r'"location":\s*"([^"]*)"', body)
        sm_loc_match = re.search(r'"sm_location":\s*"([^"]*)"', body)
        acc_match = re.search(r'"account":\s*"([^"]*)"', body)
        sub_match = re.search(r'"custcol_subledger":\s*"([^"]*)"', body)
        
        vendors.append({
            "vendor_name": name,
            "entity": entity_match.group(1) if entity_match else "",
            "department": dept_match.group(1) if dept_match else "",
            "location": loc_match.group(1) if loc_match else "",
            "sm_location": sm_loc_match.group(1) if sm_loc_match else "",
            "account": acc_match.group(1) if acc_match else "",
            "custcol_subledger": sub_match.group(1) if sub_match else ""
        })
    return vendors

def parse_excel_file(filepath):
    if not os.path.exists(filepath):
        print(f"Excel file not found: {filepath}")
        return []
        
    print(f"Parsing Excel from: {filepath}")
    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f"Failed to read Excel file: {e}")
        return []
        
    df = df.fillna("")
    records = []
    
    for _, row in df.iterrows():
        vendor_name = str(row.get('vendor name', '')).strip()
        if not vendor_name:
            continue
            
        entity = str(row.get('Entity code', '')).strip()
        if entity.endswith('.0'):
            entity = entity[:-2]
            
        account = str(row.get('Account', '')).strip()
        if account.endswith('.0'):
            account = account[:-2]
            
        subledger = str(row.get('custcol_subledger', '')).strip()
        if subledger.endswith('.0'):
            subledger = subledger[:-2]
            
        location_raw = str(row.get('location', '')).strip()
        if location_raw.endswith('.0'):
            location_raw = location_raw[:-2]
            
        sm_location_raw = str(row.get('sm_location', '')).strip()
        if sm_location_raw.endswith('.0'):
            sm_location_raw = sm_location_raw[:-2]
            
        # Split locations and sm_locations (e.g. "47/57" and "353/358")
        locs = [l.strip() for l in location_raw.split('/') if l.strip()]
        sm_locs = [s.strip() for s in sm_location_raw.split('/') if s.strip()]
        
        if not locs:
            locs = [""]
        if not sm_locs:
            sm_locs = [""]
            
        max_len = max(len(locs), len(sm_locs))
        for i in range(max_len):
            loc = locs[i] if i < len(locs) else ""
            sm_loc = sm_locs[i] if i < len(sm_locs) else ""
            
            records.append({
                "vendor_name": vendor_name,
                "entity": entity,
                "department": "5",  # Default to "5" for all Excel-sourced data
                "location": loc,
                "sm_location": sm_loc,
                "account": account,
                "custcol_subledger": subledger
            })
            
    print(f"Parsed {len(records)} branch rows from Excel.")
    return records

def main():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    master_file = os.path.join(dir_path, "sundarams_vendor_master.py")
    
    # We look for updated_data.xlsx at backend root
    backend_root = os.path.abspath(os.path.join(dir_path, "..", "..", ".."))
    excel_file = os.path.join(backend_root, "updated_data.xlsx")

    # 1. Parse both sources
    py_vendors = parse_vendor_master_file(master_file)
    excel_vendors = parse_excel_file(excel_file)
    
    all_vendors = py_vendors + excel_vendors
    print(f"Total raw records parsed (Python: {len(py_vendors)}, Excel: {len(excel_vendors)}): {len(all_vendors)}")

    # 2. Merge and de-duplicate based on case-insensitive vendor_name, location, and sm_location
    seen = set()
    unique_vendors = []
    
    for v in all_vendors:
        key = (v["vendor_name"].strip().upper(), v["location"].strip(), v["sm_location"].strip())
        if key not in seen:
            seen.add(key)
            unique_vendors.append(v)
            
    print(f"De-duplicated records to insert: {len(unique_vendors)} (Removed {len(all_vendors) - len(unique_vendors)} duplicates)")

    if not unique_vendors:
        print("No vendor mappings found to insert. Aborting database load.")
        sys.exit(1)

    pg_config = load_db_config()
    if not pg_config:
        sys.exit(1)

    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=pg_config.get('database', 'document_pipeline'),
            user=pg_config.get('user'),
            password=pg_config.get('password')
        )
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        sys.exit(1)

    cursor = conn.cursor()

    # Step 3: Re-create Table (Dropping old table to refresh it)
    print("Dropping table 'sundarams_vendor_master' to rebuild it...")
    try:
        cursor.execute("DROP TABLE IF EXISTS sundarams_vendor_master CASCADE;")
        conn.commit()
    except Exception as e:
        print(f"Error dropping table: {e}")
        conn.rollback()

    print("Creating table 'sundarams_vendor_master'...")
    create_table_query = """
    CREATE TABLE sundarams_vendor_master (
        s_no SERIAL PRIMARY KEY,
        vendor_name VARCHAR(255) NOT NULL,
        entity VARCHAR(100),
        department VARCHAR(100),
        location VARCHAR(100),
        sm_location VARCHAR(100),
        account VARCHAR(100),
        custcol_subledger VARCHAR(100)
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'sundarams_vendor_master' ready.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Step 4: Populate Data
    print(f"Inserting {len(unique_vendors)} unique vendor records into database...")
    inserted_count = 0
    
    insert_query = """
    INSERT INTO sundarams_vendor_master (
        vendor_name, entity, department, location, sm_location, account, custcol_subledger
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    for v in unique_vendors:
        try:
            cursor.execute(insert_query, (
                v["vendor_name"],
                v["entity"],
                v["department"],
                v["location"],
                v["sm_location"],
                v["account"],
                v["custcol_subledger"]
            ))
            inserted_count += 1
        except Exception as e:
            print(f"Error inserting vendor '{v['vendor_name']}' branch '{v['location']}/{v['sm_location']}': {e}")
            conn.rollback()
            conn.close()
            sys.exit(1)

    try:
        conn.commit()
        print(f"Database setup complete! Successfully inserted {inserted_count} vendor branch mappings.")
    except Exception as e:
        print(f"Error committing transaction: {e}")
        conn.rollback()
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
