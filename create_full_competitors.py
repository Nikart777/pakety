import pandas as pd
import os

def generate_competitors_template():
    if not os.path.exists('price.xlsx'):
        print("❌ price.xlsx not found. Cannot generate template.")
        return

    try:
        df_price = pd.read_excel('price.xlsx')
    except Exception as e:
        print(f"❌ Error reading price.xlsx: {e}")
        return

    # Extract unique Zone + Tariff combinations
    # Clean names
    df_price['Название'] = df_price['Название'].astype(str).str.strip()
    df_price['Тариф'] = df_price['Тариф'].astype(str).str.strip()

    unique_pairs = df_price[['Название', 'Тариф']].drop_duplicates()

    rows = []

    for _, row in unique_pairs.iterrows():
        zone = row['Название']
        tariff = row['Тариф']

        # 1. Base Row
        rows.append({
            "Ваша Зона": zone,
            "Тариф": tariff,
            "Цена Конкурента 1": None,
            "Цена Конкурента 2": None,
            "Ваш Коэффициент": 1.0
        })

        # 2. Weekday Row (Будни)
        rows.append({
            "Ваша Зона": zone,
            "Тариф": f"{tariff} (Будни)",
            "Цена Конкурента 1": None,
            "Цена Конкурента 2": None,
            "Ваш Коэффициент": 1.0
        })

        # 3. Weekend Row (Выходные)
        rows.append({
            "Ваша Зона": zone,
            "Тариф": f"{tariff} (Выходные)",
            "Цена Конкурента 1": None,
            "Цена Конкурента 2": None,
            "Ваш Коэффициент": 1.0
        })

    df_comp = pd.DataFrame(rows)
    df_comp.to_excel('competitors.xlsx', index=False)
    print(f"✅ Created competitors.xlsx with {len(rows)} rows based on your Price list.")

if __name__ == "__main__":
    generate_competitors_template()
