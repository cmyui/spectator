#!/usr/bin/env python3
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Awaitable
from typing import Literal
from typing import Mapping

import httpx

import settings
import hosts


@dataclass
class Authorization:
    api_token: str
    api_token_expiry: datetime


@dataclass
class Ratelimit:
    requests_per_period: int
    period_length: int  # in seconds


@dataclass
class RatelimitTracker:
    rate_limit: Ratelimit
    period_start: datetime
    requests_made_in_period: int = 0

    def seconds_until_reset(self) -> float:
        end_of_period = self.period_start + timedelta(
            seconds=self.rate_limit.period_length
        )
        return (end_of_period - datetime.now()).total_seconds()

    def hit_rate_limit(self) -> bool:
        if self.seconds_until_reset() <= 0:
            return False

        return self.requests_made_in_period >= self.rate_limit.requests_per_period

    def record_request(self) -> None:
        self.requests_made_in_period += 1


OSU_API_V2_RATE_LIMIT = Ratelimit(
    # https://osu.ppy.sh/docs/index.html#terms-of-use
    requests_per_period=500,
    period_length=60,
)

http_client: httpx.AsyncClient
authorization: Authorization | None = None
rate_limit_tracker: RatelimitTracker | None = None


def is_expired(authorization: Authorization) -> bool:
    # use 20 seconds of padding for bad case latency scenario
    return authorization.api_token_expiry - datetime.now() < timedelta(seconds=20)


auth_lock: asyncio.Lock = asyncio.Lock()


async def make_osu_api_v2_request(
    method: Literal[
        "HEAD",
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
        "TRACE",
    ],
    url: str,
    params: Mapping[str, Any] | None = None,
    json: Mapping[str, Any] | None = None,
) -> Any:
    global http_client, authorization

    async with auth_lock:
        if authorization:
            if is_expired(authorization):
                authorization = None

        if not authorization:
            response = await http_client.post(
                "https://osu.ppy.sh/oauth/token",
                data={
                    "client_id": settings.OSU_API_V2_CLIENT_ID,
                    "client_secret": settings.OSU_API_V2_CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "scope": "public",
                },
            )
            response.raise_for_status()

            authorization = Authorization(
                api_token=response.json()["access_token"],
                api_token_expiry=(
                    datetime.now() + timedelta(seconds=response.json()["expires_in"])
                ),
            )

    global rate_limit_tracker
    if rate_limit_tracker is None:
        rate_limit_tracker = RatelimitTracker(
            rate_limit=OSU_API_V2_RATE_LIMIT,
            period_start=datetime.now(),
        )

    if rate_limit_tracker.hit_rate_limit():
        await asyncio.sleep(rate_limit_tracker.seconds_until_reset())
        rate_limit_tracker = None
    elif rate_limit_tracker.seconds_until_reset() <= 0:
        rate_limit_tracker = None
    else:
        rate_limit_tracker.record_request()

    response = await http_client.request(
        method=method,
        url=url,
        params=params,
        json=json,
        headers={"Authorization": f"Bearer {authorization.api_token}"},
    )
    response.raise_for_status()

    return response.json()


async def resolve_user_id(username: str) -> int:
    global http_client

    # this is only available on osu!api v1 (lol)
    response = await http_client.get(
        "https://osu.ppy.sh/api/get_user",
        params={
            "u": username,
            "k": settings.API_V1_KEY,
        },
    )
    response.raise_for_status()

    user_id = int(response.json()[0]["user_id"])
    return user_id


async def download_map(beatmapset_id: int) -> None:
    response = await http_client.get(
        f"https://api.chimu.moe/v1/download/{beatmapset_id}",
        # f"https://us.kitsu.moe/api/d/{beatmapset_id}",
        follow_redirects=True,
    )
    response.raise_for_status()

    beatmap_file_content = response.read()
    with open(f"beatmapsets/{beatmapset_id}.osz", "wb") as f:
        f.write(beatmap_file_content)


async def get_user_recent_scores(
    user_id: int,
    include_fails: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    return await make_osu_api_v2_request(
        method="GET",
        url=f"https://osu.ppy.sh/api/v2/users/{user_id}/scores/recent",
        params={
            "include_fails": include_fails,
            "limit": limit,
            "offset": offset,
        },
    )


def should_download(score: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    return (
        score["beatmap"]["mode"] == config["game_mode"]
        and (
            config["star_rating"]["min"]
            <= score["beatmap"]["difficulty_rating"]
            <= config["star_rating"]["max"]
        )
        and (
            config["approach_rate"]["min"]
            <= score["beatmap"]["ar"]
            <= config["approach_rate"]["max"]
        )
        and score["beatmapset"]["id"] not in downloaded_beatmapsets
    )


async def download_user_maps(user_id: int, config: Mapping[str, Any]) -> None:
    tasks: list[Awaitable[Any]] = []

    for score in await get_user_recent_scores(user_id):
        if should_download(score, config):
            beatmapset_id = score["beatmapset"]["id"]
            tasks.append(asyncio.create_task(download_map(beatmapset_id)))
            downloaded_beatmapsets.append(score["beatmapset"]["id"])

    await asyncio.gather(*tasks)


def get_currently_downloaded_beatmapsets() -> list[int]:
    return [int(path.removesuffix(".osz")) for path in os.listdir("beatmapsets")]


async def main() -> int:
    if not os.path.exists("beatmapsets"):
        os.mkdir("beatmapsets")

    global downloaded_beatmapsets
    downloaded_beatmapsets = get_currently_downloaded_beatmapsets()

    global http_client
    http_client = httpx.AsyncClient()

    user_ids = await asyncio.gather(
        *[
            asyncio.create_task(resolve_user_id(config["username"]))
            for config in hosts.configs
        ]
    )

    await asyncio.gather(
        *[
            asyncio.create_task(download_user_maps(user_id, config))
            for user_id, config in zip(user_ids, hosts.configs)
        ]
    )

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    raise SystemExit(exit_code)
