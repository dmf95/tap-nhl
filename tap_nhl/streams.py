"""Stream type classes for tap-nhl."""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_nhl.client import nhlStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class GamesStream(nhlStream):
    """Define custom stream."""
    name = "schedule"
    path = "/schedule"
    primary_keys = ["gamePk"]
    replication_key = None
    records_jsonpath = "$.dates[*].games[*]"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("gamePk", th.IntegerType),
        th.Property("link", th.StringType),
        th.Property("gameType", th.StringType),
        th.Property("season", th.StringType),
        th.Property("gameDate", th.DateTimeType),
        th.Property("status", th.ObjectType(
            th.Property("abstractGameState", th.StringType),
            th.Property("codedGameState", th.StringType),
            th.Property("detailedState", th.StringType),
            th.Property("statusCode", th.StringType),
            th.Property("startTimeTBD", th.BooleanType),
        )),
        th.Property("teams", th.ObjectType(
            th.Property("away", th.ObjectType(
                th.Property("leagueRecord", th.ObjectType(
                    th.Property("wins", th.IntegerType),
                    th.Property("losses", th.IntegerType),
                    th.Property("ot", th.IntegerType),
                    th.Property("type", th.StringType),
                )),
                th.Property("score", th.IntegerType),
                th.Property("team", th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("name", th.StringType),
                    th.Property("link", th.StringType),
                ))
            )),
            th.Property("home", th.ObjectType(
                th.Property("leagueRecord", th.ObjectType(
                    th.Property("wins", th.IntegerType),
                    th.Property("losses", th.IntegerType),
                    th.Property("ot", th.IntegerType),
                    th.Property("type", th.StringType),
                )),
                th.Property("score", th.IntegerType),
                th.Property("team", th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("name", th.StringType),
                    th.Property("link", th.StringType),
                ))
            )),
        )),
        th.Property("venue", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("link", th.StringType),
        )),
        th.Property("content", th.ObjectType(
            th.Property("link", th.StringType)
        ))
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        params.update(
            {
                "date": self.config.get("start_date")
            }
        )
        return params
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "game_id": record["gamePk"]
        }

class LiveDataStream(nhlStream):
    name = "live_feed"
    path = "/game"
    primary_keys = ["gamePk"]
    records_jsonpath = "$.liveData"
    replication_key = None
    parent_stream_type = GamesStream
    ignore_parent_replication_keys = True
    path = "/game/{game_id}/feed/live"

    schema = th.PropertiesList(
        th.Property("plays", th.ObjectType()),
        th.Property("linescore", th.ObjectType()),
        th.Property("boxscore", th.ObjectType()),
        th.Property("decisions", th.ObjectType())
    ).to_dict()
