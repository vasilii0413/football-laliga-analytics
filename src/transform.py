import pandas as pd
from pathlib import Path

CLEANED_PATH = Path("../data/cleaned/players_cleaned.csv")
TRANSFORMED_PLAYERS_PATH = Path("../data/transformed/players_transformed.csv")
TEAM_AGG_PATH = Path("../data/transformed/teams_aggregated.csv")


def load_cleaned():
    if not CLEANED_PATH.exists():
        raise FileNotFoundError("Cleaned file not found. Run clean.py first!")

    return pd.read_csv(CLEANED_PATH)


def add_player_metrics(df):
    df["PS%"] = pd.to_numeric(df["PS%"], errors="coerce")

    df["MinP"] = df["MinP"].replace(0, pd.NA)

    # Goals per 90
    df["Goals_per90"] = (df["Goals"] / df["MinP"]) * 90

    # Assists per 90
    df["Assists_per90"] = (df["Assist"] / df["MinP"]) * 90

    # Goal involvement per 90
    df["GI_per90"] = df["Goals_per90"] + df["Assists_per90"]

    # Cars per 90
    df["Cards_per90"] = ((df["YC"] + df["RC"]) / df["MinP"]) * 90

    # Normalize rating to 0-1
    df["Rating_norm"] = df["Rating"] / df["Rating"].max()

    # Pass success category
    df["PS_Category"] = pd.cut(
        df["PS%"],
        bins=[0, 70, 80, 90, 100],
        labels=["Poor", "Average", "Good", "Excellent"]
    )

    # Replace NAN where necessary
    df.fillna({
        "Goals_per90": 0,
        "Assists_per90": 0,
        "GI_per90": 0,
        "Cards_per90": 0,
    }, inplace=True)

    return df


def build_team_stats(df):
    """Building stats of every team"""
    df["MinP"] = df["MinP"].replace(0, pd.NA)
    df["Goals_per90"] = (df["Goals"] / df["MinP"]) * 90
    team_df = df.groupby("Team-name").agg(
        Players=("Player Name", "count"),
        Avg_Age=("Age", "mean"),
        Total_Goals=("Goals", "sum"),
        Avg_Goals_per90=("Goals_per90", "mean"),
        Total_Assists=("Assist", "sum"),
        Avg_rating=("Rating", "mean"),
        Total_minutes=("MinP", "sum"),
        Total_YC=("YC", "sum"),
        Total_RC=("RC", "sum"),
    ).reset_index()

    return team_df


def save_output(players_df, team_df):
    players_df, team_df = players_df.round(2), team_df.round(2)

    TRANSFORMED_PLAYERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    players_df.to_csv(TRANSFORMED_PLAYERS_PATH, index=False)
    team_df.to_csv(TEAM_AGG_PATH, index=False)

    print(f"Player dataset saved to: {TRANSFORMED_PLAYERS_PATH}")
    print(f"Team dataset saved to: {TEAM_AGG_PATH}")


def main():
    df = load_cleaned()
    df = add_player_metrics(df)
    team_df = build_team_stats(df)
    save_output(df, team_df)


if __name__ == "__main__":
    main()
