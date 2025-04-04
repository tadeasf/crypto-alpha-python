"""
WebSocket endpoints for real-time market data streaming.
"""
from typing import Dict, List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.exchange import exchange_service
from crypto_alpha_python.services.collector import MarketDataCollector

router = APIRouter()

class ConnectionManager:
    """Manager for WebSocket connections."""
    
    def __init__(self):
        """Initialize the connection manager."""
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, symbol: str):
        """Connect a WebSocket client."""
        await websocket.accept()
        if symbol not in self.active_connections:
            self.active_connections[symbol] = []
        self.active_connections[symbol].append(websocket)
    
    def disconnect(self, websocket: WebSocket, symbol: str):
        """Disconnect a WebSocket client."""
        if symbol in self.active_connections:
            self.active_connections[symbol].remove(websocket)
            if not self.active_connections[symbol]:
                del self.active_connections[symbol]
    
    async def broadcast(self, message: str, symbol: str):
        """Broadcast message to all connected clients for a symbol."""
        if symbol in self.active_connections:
            for connection in self.active_connections[symbol]:
                await connection.send_text(message)

manager = ConnectionManager()

@router.websocket("/ws/market-data/{symbol}")
async def websocket_endpoint(
    websocket: WebSocket,
    symbol: str,
    session: AsyncSession = Depends(get_session),
):
    """
    WebSocket endpoint for real-time market data.
    """
    await manager.connect(websocket, symbol)
    
    try:
        # Start market data collection
        collector = MarketDataCollector(session)
        await collector.start_collection([symbol])
        
        # Connect to exchange WebSocket
        await exchange_service.connect_binance_websocket(
            symbol,
            lambda msg: manager.broadcast(msg, symbol),
        )
        
        while True:
            # Keep the connection alive
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)
    finally:
        await collector.stop_collection()
        await exchange_service.close_connections() 