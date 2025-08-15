# WITh try error so it can be run even if one fails to find the materialized view
#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def refresh_view(engine, fq_name):
    try:
        # Begin a transaction just for this refresh
        with engine.begin() as conn:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {fq_name}"))
        print(f"✅ Refreshed: {fq_name}")
    except Exception as e:
        print(f"❌ Failed to refresh {fq_name}: {e}")

def main():
    load_dotenv()

    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise RuntimeError("Please set the DATABASE_URL environment variable")

    engine = create_engine(DATABASE_URL)

    materialized_views = [
        "dim_customers",
        "dim_payment_types",
        "dim_products",
        "dim_sellers",
        "fact_customer_satisfaction",
        "fact_customer_segmentation",
        "fact_issues_mv",
        "product_daily_revenue_mv",
        "sales_table_v2",
        "mv_certified_status",
    ]

    for mv in materialized_views:
        fq = f"public.{mv}"
        refresh_view(engine, fq)

if __name__ == "__main__":
    main()



# from sqlalchemy import create_engine, text
# import os

# # Load DB credentials
# DATABASE_URL = os.getenv("DATABASE_URL")
# if not DATABASE_URL:
#     raise RuntimeError("Please set the DATABASE_URL environment variable")

# engine = create_engine(DATABASE_URL)

# # List all the materialized views you want to refresh
# materialized_views = [
    
#     "dim_customers",
#     "dim_payment_types",
#     "dim_products",
#     "dim_sellers",
#     "fact_customer_satisfaction",
#     "fact_customer_segmentation",
#     "fact_issues_mv",
#     "product_daily_revenue_mv",
#     "sales_table_v2",
#     # add any others here…
# ]

# with engine.begin() as conn:
#     for mv in materialized_views:
#         fq_name = f"public.{mv}"
#         conn.execute(text(f"REFRESH MATERIALIZED VIEW {fq_name}"))
#         print(f"✅ {mv} refreshed.")


# this one is for one at a time
# from sqlalchemy import create_engine, text
# import os

# # Load DB credentials
# DATABASE_URL = os.getenv("DATABASE_URL")
# if not DATABASE_URL:
#     raise RuntimeError("Please set the DATABASE_URL environment variable")

# engine = create_engine(DATABASE_URL)

# with engine.begin() as conn:
#     conn.execute(text("REFRESH MATERIALIZED VIEW public.fact_issues_mv"))
#     print("✅ fact_customer_segmentation refreshed.")
