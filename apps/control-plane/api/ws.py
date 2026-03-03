from fastapi import WebSocket

connections = []

async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connections.append(ws)

    while True:
        await ws.receive_text()