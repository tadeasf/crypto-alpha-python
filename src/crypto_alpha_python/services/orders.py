"""
Orders service for handling order operations.
"""
from datetime import datetime
from typing import List
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from uuid import UUID

from crypto_alpha_python.models.orders import Order, OrderCreate, OrderStatus

async def create_order(
    session: AsyncSession,
    order: OrderCreate,
) -> Order:
    """
    Create a new order.
    """
    db_order = Order.from_orm(order)
    session.add(db_order)
    await session.commit()
    await session.refresh(db_order)
    return db_order

async def get_order(
    session: AsyncSession,
    order_id: UUID,
) -> Order | None:
    """
    Get order by ID.
    """
    query = select(Order).where(Order.order_id == order_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_user_orders(
    session: AsyncSession,
    user_id: UUID,
) -> List[Order]:
    """
    Get all orders for a user.
    """
    query = select(Order).where(Order.user_id == user_id)
    result = await session.execute(query)
    return result.scalars().all()

async def update_order_status(
    session: AsyncSession,
    order_id: UUID,
    status: str,
) -> Order | None:
    """
    Update order status.
    """
    order = await get_order(session=session, order_id=order_id)
    if not order:
        return None
    
    try:
        order.status = OrderStatus(status)
        order.updated_at = datetime.utcnow()
        
        if order.status == OrderStatus.FILLED:
            order.filled_at = datetime.utcnow()
        
        await session.commit()
        await session.refresh(order)
        return order
    except ValueError:
        return None 