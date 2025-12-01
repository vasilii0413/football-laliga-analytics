import pandas as pd
from pathlib import Path

RAW_PATH = Path("../data/raw/laliga_all_players_stats.csv")
PROCESSED_PATH = Path("../data/processed/players_clean.csv")

required_columns = [
    "Player Name",
    "Team-name",
    "Age",
    "Position",
    "App",
    "MinP",
    "Goals",
    "Assists",
    "YC",
    "RC",
    "SPG",
    "PS%",
    "AW",
    "MOTM",
    "Rating"
]


def load_raw():
    if not RAW_PATH.exists():
        raise FileNotFoundError("Raw laliga_all_players_stats.csv not found")

    return pd.read_csv(RAW_PATH)


def basic_clean(df):
    df.rename(columns={
        "Team": "Team-name",
        "Appearances": "App",
        "Minutes played": "MinP",
        "Shots per game": "SPG",
        "Pass success": "PS%",
        "Aerials won": "AW",
        "Assist": "Assists"  # important fix!
    }, inplace=True)

    df = df.dropna(how="all")
    df.columns = [col.strip() for col in df.columns]
    return df


def validate_columns(df):
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")


def save_processed(df):
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)
    print(f"Processed file saved to: {PROCESSED_PATH}")


def main():
    df = load_raw()
    df = basic_clean(df)
    validate_columns(df)
    save_processed(df)


if __name__ == "__main__":
    main()
