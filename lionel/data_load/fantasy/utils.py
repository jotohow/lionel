class DataLoadConfig:

    FIXTURE_COL_MAP = {
        "event": "gameweek",
        "team_h": "home_id",
        "team_a": "away_id",
        "team_h_score": "home_score",
        "team_a_score": "away_score",
        "id": "fixture_season_id",
    }

    GW_COL_MAP = {'id': 'gameweek_id'}

    TEAM_MAP = {
        "team_name": {
            "Man City": "Manchester City",
            "Man Utd": "Manchester Utd",
            "Spurs": "Tottenham",
            "Nott'm Forest": "Nottingham",
        }
    }

    PLAYER_MAP = {
        "full_name": {"Dominic Solanke-Mitchell": "Dominic Solanke"},
        "name": {
            "Philip": "Billing",
            "J.Ramsey": "Ramsey",
            "Cook": "L.Cook",
            "H.Traorè": "Hamed Traorè",
            "Kozłowski": "Kozlowski",
            "Sánchez": "Sanchez",
            "Andrey Santos": "Andrey",
            "Vinicius": "Vinícius",
            "N.Phillips": "Phillips",
            "M.Salah": "Salah",
            "N.Willians": "Williams",
            "P.M.Sarr": "Sarr",
            "J.Gomes": "João Gomes",
        },
    }
