import asyncio, json, websockets

async def fake_alert_server():
    async with websockets.serve(handler, "0.0.0.0", 8765):
        print("Servidor WS de teste rodando em ws://localhost:8765")
        await asyncio.Future()  # roda para sempre

async def handler(ws):
    try:
        while True:
            # cria um alerta fake
            alert = {
                "type": "alerts",
                "items": [
                    {
                        "ts": "2025-09-16T03:00:00Z",
                        "actuator_id": 2,
                        "type": "sensor_falha",
                        "severity": "high",
                        "message": "Sensor de posição não respondeu"
                    }
                ]
            }
            await ws.send(json.dumps(alert))
            await asyncio.sleep(10)  # repete a cada 10s
    except websockets.exceptions.ConnectionClosed:
        print("Cliente desconectado.")

if __name__ == "__main__":
    asyncio.run(fake_alert_server())
