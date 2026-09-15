import asyncio
import json
import logging
import os
from http import HTTPStatus

import websockets


class IgnoreHandshakeError(logging.Filter):
    # RenderのHEADリクエスト(死活監視)による「opening handshake failed」のエラーログを表示しない
    def filter(self, record):
        return record.getMessage() != "opening handshake failed"


logging.getLogger("websockets.server").addFilter(IgnoreHandshakeError())

players = {}
clients = {}
next_player_id = 1


async def send_json(websocket, data):
    await websocket.send(json.dumps(data))


async def broadcast(data, exclude_websocket=None):
    message = json.dumps(data)

    disconnected_clients = []

    for websocket in clients:
        if websocket == exclude_websocket:
            continue

        try:
            await websocket.send(message)
        except:
            disconnected_clients.append(websocket)

    for websocket in disconnected_clients:
        clients.pop(websocket, None)


async def handler(websocket):
    global next_player_id

    player_id = next_player_id
    next_player_id += 1

    clients[websocket] = player_id

    players[str(player_id)] = {
        "x": 0,
        "z": 0
    }

    print(f"Player {player_id} joined", flush=True)

    await send_json(websocket, {
        "type": "joined",
        "player_id": player_id,
        "players": players
    })

    await broadcast({
        "type": "player_joined",
        "player_id": player_id,
        "x": 0,
        "z": 0
    }, exclude_websocket=websocket)

    try:
        async for message in websocket:
            data = json.loads(message)

            if data.get("type") == "move":
                x = data.get("x", 0)
                z = data.get("z", 0)

                players[str(player_id)] = {
                    "x": x,
                    "z": z
                }

                await broadcast({
                    "type": "player_update",
                    "player_id": player_id,
                    "x": x,
                    "z": z
                }, exclude_websocket=websocket)

    except websockets.exceptions.ConnectionClosed:
        pass

    finally:
        print(f"Player {player_id} left", flush=True)

        clients.pop(websocket, None)
        players.pop(str(player_id), None)

        await broadcast({
            "type": "player_left",
            "player_id": player_id
        })


def process_request(connection, request):
    # WebSocket以外の通常のHTTPアクセス(Renderのヘルスチェックやブラウザ)には "OK" を返す
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(HTTPStatus.OK, "OK\n")
    return None


async def main():
    # Renderは環境変数PORTでポート番号を指定する(ローカルでは8765)
    port = int(os.environ.get("PORT", 8765))
    print(f"WebSocket server started: ws://0.0.0.0:{port}", flush=True)
    async with websockets.serve(handler, "0.0.0.0", port, process_request=process_request):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())