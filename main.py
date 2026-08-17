from urllib.parse import unquote_plus

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Stremio Proxy")

PROXY_CHUNK_SIZE = 64 * 1024

TIMEOUT = httpx.Timeout(
    connect=15.0,
    read=None,
    write=30.0,
    pool=30.0,
)

LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=20,
)

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

BLOCKED_REQUEST_HEADERS = {
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
}


def parse_stremio_proxy_url(path: str):
    """
    Parse Stremio proxy URLs such as:

    /proxy/d=https%3A%2F%2Fbcdnxw.hakunaymatata.com
    &h=User-Agent%3AMozilla%2F5.0...
    &h=Referer%3Ahttps%3A%2F%2Flok-lok.cc%2F
    /bt/file.mp4?sign=...&t=...

    Stremio can put the actual destination path after the h= parameters.
    """

    if not path.startswith("/proxy/"):
        return None, {}

    payload = path[len("/proxy/") :]

    if not payload.startswith("d="):
        return None, {}

    payload = payload[2:]

    header_marker = "&h="

    marker_position = payload.find(header_marker)

    if marker_position == -1:
        return unquote_plus(payload), {}

    destination_base = payload[:marker_position]

    remainder = payload[marker_position:]

    # Find the first RAW "/" after the header declarations.
    #
    # Header URLs contain encoded slashes (%2F), so a raw "/" indicates
    # that Stremio has started appending the destination path.
    path_position = remainder.find("/")

    if path_position == -1:
        header_section = remainder
        destination_suffix = ""
    else:
        header_section = remainder[:path_position]
        destination_suffix = remainder[path_position:]

    headers = {}

    for item in header_section.split("&h="):
        if not item:
            continue

        decoded = unquote_plus(item)

        if ":" not in decoded:
            continue

        name, value = decoded.split(":", 1)

        name = name.strip()
        value = value.strip()

        if not name:
            continue

        headers[name] = value

    destination = unquote_plus(destination_base) + destination_suffix

    return destination, headers


def filter_response_headers(headers: httpx.Headers):
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }


@app.get("/")
async def root():
    return {
        "name": "Stremio Proxy",
        "status": "ok",
    }


@app.api_route(
    "/proxy/{proxy_path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
    ],
)
async def proxy(request: Request, proxy_path: str):

    raw_path = request.scope["raw_path"].decode("utf-8")

    destination, stremio_headers = parse_stremio_proxy_url(raw_path)

    if not destination:
        return JSONResponse(
            {
                "detail": (
                    "Invalid Stremio proxy URL. "
                    "Expected /proxy/d=<url>&h=Header%3AValue"
                )
            },
            status_code=400,
        )

    headers = {}

    headers.update(stremio_headers)

    if range_header := request.headers.get("range"):
        headers["Range"] = range_header

    body = await request.body()

    client = httpx.AsyncClient(
        timeout=TIMEOUT,
        limits=LIMITS,
        follow_redirects=True,
    )

    req = client.stream(
        method=request.method,
        url=destination,
        headers=headers,
        content=body,
        params=request.query_params,
    )

    try:
        upstream = await req.__aenter__()

    except httpx.ConnectError:
        await client.aclose()

        return JSONResponse(
            {"detail": "Could not connect to upstream"},
            status_code=502,
        )

    except httpx.TimeoutException:
        await client.aclose()

        return JSONResponse(
            {"detail": "Upstream timed out"},
            status_code=504,
        )

    except httpx.HTTPError as exc:
        await client.aclose()

        return JSONResponse(
            {
                "detail": "Upstream request failed",
                "error": str(exc),
            },
            status_code=502,
        )

    if upstream.status_code not in (200, 206) and request.method == "GET":
        await req.__aexit__(None, None, None)
        await client.aclose()

        return JSONResponse(
            {"detail": "Upstream error"},
            status_code=upstream.status_code,
        )

    response_headers = filter_response_headers(upstream.headers)

    async def stream():
        try:
            async for chunk in upstream.aiter_raw(chunk_size=PROXY_CHUNK_SIZE):
                yield chunk
        finally:
            await req.__aexit__(None, None, None)
            await client.aclose()

    return StreamingResponse(
        stream(),
        status_code=upstream.status_code,
        headers=response_headers,
        media_type=upstream.headers.get("content-type"),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=11470,
        reload=True,
    )
