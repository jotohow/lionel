"""Typed candidate record for the selectors, and the DataFrame adapter."""

from collections.abc import Iterable, Mapping
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator

PlayerId = int | str
Position = Literal["GK", "DEF", "MID", "FWD"]

# Field name -> default DataFrame column name.
DEFAULT_COLUMNS: dict[str, str] = {
    "player_id": "player",
    "team": "team",
    "position": "position",
    "price": "price",
    "predicted_points": "predicted_points",
    "name": "name",
    "xv": "xv",
    "xi": "xi",
    "captain": "captain",
}

REQUIRED_FIELDS = ("player_id", "team", "position", "price", "predicted_points")
OPTIONAL_FIELDS = ("name", "xv", "xi", "captain")


def coerce_player_id(value: object) -> object:
    """A string of digits becomes an int, so "123" and 123 are the same id."""
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value


class Player(BaseModel):
    """One selection candidate.

    `player_id` is an int or a str. A str of digits becomes an int, so "123" and 123
    are the same id. `name` is an optional display name, for example `web_name`.

    The flags are input and output. On input, `xv` marks the current squad
    (used by `UpdateXVSelector`). On output, `xv`, `xi` and `captain` mark the
    selection.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    player_id: PlayerId
    team: str
    position: Position
    price: float
    predicted_points: float
    name: str | None = None
    xv: bool = False
    xi: bool = False
    captain: bool = False

    @field_validator("player_id", mode="before")
    @classmethod
    def _digits_to_int(cls, value: object) -> object:
        return coerce_player_id(value)


def resolve_columns(columns: Mapping[str, str] | None = None) -> dict[str, str]:
    """Merge a user column mapping over the default mapping."""
    columns = dict(columns or {})
    unknown = set(columns) - set(DEFAULT_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown fields in columns: {sorted(unknown)}. Valid fields: {sorted(DEFAULT_COLUMNS)}.")
    return {**DEFAULT_COLUMNS, **columns}


def validate_players(players: Iterable[Player | Mapping]) -> list[Player]:
    """Validate records or dicts and reject duplicate ids. Errors name the list index."""
    result: list[Player] = []
    seen: set[PlayerId] = set()
    for i, record in enumerate(players):
        try:
            player = Player.model_validate(record)
        except ValueError as e:
            raise ValueError(f"Invalid candidate at index {i}: {e}") from e
        if player.player_id in seen:
            raise ValueError(f"Duplicate player_id: {player.player_id!r} (index {i}).")
        seen.add(player.player_id)
        result.append(player)
    return result


def players_from_dataframe(df: pd.DataFrame, columns: Mapping[str, str] | None = None) -> list[Player]:
    """Convert a DataFrame to a list of `Player`, in row order."""
    columns = resolve_columns(columns)
    missing = [f"{field} (column {columns[field]!r})" for field in REQUIRED_FIELDS if columns[field] not in df.columns]
    if missing:
        raise ValueError(f"candidate DataFrame is missing required columns: {', '.join(missing)}.")

    used = {f: columns[f] for f in REQUIRED_FIELDS}
    used.update({f: columns[f] for f in OPTIONAL_FIELDS if columns[f] in df.columns})
    records = df[list(used.values())].set_axis(list(used), axis=1).to_dict("records")
    return validate_players(records)
