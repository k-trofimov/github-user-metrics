from consts.consts import GAINED_OWNERSHIP_EVENTS, LOST_OWNERSHIP_EVENTS
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def repository_ownership_incr(event_type: str) -> int:
    if event_type in GAINED_OWNERSHIP_EVENTS:
        return 1
    if event_type in LOST_OWNERSHIP_EVENTS:
        return -1
    else:
        return 0


def commits_incr(event_type: str, payload: dict) -> int:
    if event_type == "PushEvent":
        return len(payload["commits"])
    else:
        return 0

commits_incr_udf = udf(commits_incr, IntegerType())
rep_own_incr_udf = udf(repository_ownership_incr, IntegerType())