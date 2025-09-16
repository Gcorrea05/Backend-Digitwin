# mock_backend.py
import asyncio, json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI()

@app.get("/health")
def health():
    return {"redis_ok": True, "db_ok": True}

@app.get("/api/live/actuators/state")
def acts():
    return {"actuators":[{"actuator_id":1,"state":"ok"},{"actuator_id":2,"state":"erro"}]}

@app.get("/api/live/cycles/rate")
def cpm(actuator_id: int | None = None):
    if actuator_id == 1: return {"cpm": 23.4}
    if actuator_id == 2: return {"cpm": 0.0}
    return {"rates": {"1": 23.4, "2": 0.0}}

@app.get("/api/live/vibration")
def vib(actuator_id: int | None = None):
    if actuator_id == 1: return {"avg": 3.1}
    if actuator_id == 2: return {"avg": 7.8}
    return {"by_actuator": {"1": 3.1, "2": 7.8}}

@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            # manda um alerta a cada 10s
            msg = {
                "type":"alerts",
                "items":[
                    {"ts":"2025-09-16T01:00:00Z","actuator_id":2,"type":"sensor_falha","severity":"high","message":"Sensor de posição não respondeu"}
                ]
            }
            await ws.send_text(json.dumps(msg))
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
