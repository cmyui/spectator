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

# TODO: abstract this out of main file (ideally, out of python)
configs = [
    {
        "username": "mrekk",
        "game_mode": "osu",
        "star_rating": {"min": 5.0, "max": 13.0},
        "approach_rate": {"min": 1.0, "max": 10.0},
        # "overall_difficulty": {"min": 5.0, "max": 10.0},
        # "circle_size": {"min": 5.0, "max": 10.0},
        # "health_points": {"min": 5.0, "max": 10.0},
        # "song_length": {"min": 180.0, "max": 240.0},
    },
    {
        "username": "Justice",
        "game_mode": "osu",
        "star_rating": {"min": 5.0, "max": 10.0},
        "approach_rate": {"min": 1.0, "max": 10.0},
    },
    {
        "username": "mlaw",
        "game_mode": "osu",
        "star_rating": {"min": 7.0, "max": 10.0},
        "approach_rate": {"min": 5.0, "max": 10.0},
    },
    {
        "username": "chocomint",
        "game_mode": "osu",
        "star_rating": {"min": 7.0, "max": 10.0},
        "approach_rate": {"min": 5.0, "max": 10.0},
    },
    {
        "username": "im a fancy lad",
        "game_mode": "osu",
        "star_rating": {"min": 7.0, "max": 10.0},
        "approach_rate": {"min": 5.0, "max": 10.0},
    },
]


@dataclass
class Authorization:
    api_token: str
    api_token_expiry: datetime


authorization: Authorization | None = None


def is_expired(authorization: Authorization) -> bool:
    # use 20 seconds of padding for bad case latency scenario
    return authorization.api_token_expiry - datetime.now() < timedelta(seconds=20)


auth_lock: asyncio.Lock = asyncio.Lock()


# TODO: ratelimit
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
    params: Mapping[str, Any],
    json: Mapping[str, Any],
) -> Any:
    global authorization, http_client

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

            authorization = Authorization(
                api_token=response.json()["access_token"],
                api_token_expiry=(
                    datetime.now() + timedelta(seconds=response.json()["expires_in"])
                ),
            )

    # TODO: implement ratelimit (peppy suggests 60/m, 1200/m is max)
    response = await http_client.request(
        method=method,
        url=url,
        params=params,
        json=json,
        headers={"Authorization": f"Bearer {authorization.api_token}"},
    )
    assert response.status_code in range(200, 300)

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
    assert response.status_code in range(200, 300)

    user_id = int(response.json()[0]["user_id"])
    return user_id


async def download_map(beatmapset_id: int) -> None:
    response = await http_client.get(
        f"https://api.chimu.moe/v1/download/{beatmapset_id}",
        # f"https://us.kitsu.moe/api/d/{beatmapset_id}",
        follow_redirects=True,
    )
    with open(f"beatmapsets/{beatmapset_id}.osz", "wb") as f:
        f.write(response.read())


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
        json={},
    )


def should_download(score: dict[str, Any], config: Mapping[str, Any]) -> bool:
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


async def main() -> int:
    # keep track of beatmapsets we have already downloaded
    global downloaded_beatmapsets
    downloaded_beatmapsets = [
        int(path.removesuffix(".osz")) for path in os.listdir("beatmapsets")
    ]

    global http_client
    http_client = httpx.AsyncClient()

    user_ids = await asyncio.gather(
        *[
            asyncio.create_task(resolve_user_id(config["username"]))
            for config in configs
        ]
    )

    await asyncio.gather(
        *[
            asyncio.create_task(download_user_maps(user_id, config))
            for user_id, config in zip(user_ids, configs)
        ]
    )

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    raise SystemExit(exit_code)
