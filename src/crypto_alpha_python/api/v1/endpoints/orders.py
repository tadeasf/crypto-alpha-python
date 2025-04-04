"""
Orders endpoints for handling order operations.
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession
from uuid import UUID

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.models.orders import Order, OrderCreate, OrderRead
from crypto_alpha_python.services.orders import (
    create_order,
    get_order,
    get_user_orders,
    update_order_status,
)

router = APIRouter()

@router.post("/", response_model=OrderRead)
async def create_new_order(
    order: OrderCreate,
    session: AsyncSession = Depends(get_session),
) -> Order:
    """
    Create a new order.
    """
    return await create_order(session=session, order=order)

@router.get("/{order_id}", response_model=OrderRead)
async def read_order(
    order_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> Order:
    """
    Get order by ID.
    """
    order = await get_order(session=session, order_id=order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.get("/user/{user_id}", response_model=List[OrderRead])
async def read_user_orders(
    user_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> List[Order]:
    """
    Get all orders for a user.
    """
    return await get_user_orders(session=session, user_id=user_id)

@router.patch("/{order_id}/status", response_model=OrderRead)
async def update_order(
    order_id: UUID,
    status: str,
    session: AsyncSession = Depends(get_session),
) -> Order:
    """
    Update order status.
    """
    order = await update_order_status(
        session=session,
        order_id=order_id,
        status=status,
    )
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order 