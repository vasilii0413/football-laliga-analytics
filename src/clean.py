import pandas as pd
from pathlib import Path

PROCESSED_PATH = Path("../data/processed/players_clean.csv")
CLEANED_PATH = Path("../data/cleaned/players_cleaned.csv")


def load_processed():
    if not PROCESSED_PATH.exists():
        raise FileNotFoundError("Processed file not found. Run ingest.py first!")

    return pd.read_csv(PROCESSED_PATH)


def convert_types(df):
    int_columns = ["Age", "MinP", "Goals", "Assist", "YC", "RC", "MOTM"]

    float_columns = ["SPG", "AW", "Rating"]

    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def normalize_strings(df):
    df["Player Name"] = df["Player Name"].str.strip()

    df["Team-name"] = df["Team-name"].str.strip().str.title()

    df["Position"] = df["Position"].str.upper().str.strip()

    return df


def fix_invalid_values(df):
    df = df[(df["Age"] >= 14) & (df["Age"] <= 50)]

    return df


def handle_missing_values(df):
    df.fillna({
        "Goals": 0,
        "Assist": 0,
        "YC": 0,
        "RC": 0,
        "MOTM": 0,
    }, inplace=True)

    df.dropna(subset=["Player Name", "Team-name"], inplace=True)

    return df


def save_cleaned(df):
    CLEANED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEANED_PATH, index=False)
    print(f"Cleaned dataset saved to: {CLEANED_PATH}")


def main():
    df = load_processed()
    df = convert_types(df)
    df = normalize_strings(df)
    df = fix_invalid_values(df)
    df = handle_missing_values(df)
    save_cleaned(df)


if __name__ == "__main__":
    main()
