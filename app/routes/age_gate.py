from fastapi import APIRouter, HTTPException

from ..schemas import AgeGateIn, AgeGateOut
from ..services.age_gate import verify_age

router = APIRouter()


@router.post("/verify", response_model=AgeGateOut)
def verify(payload: AgeGateIn):
    try:
        result = verify_age(payload.required_age, payload.year, payload.month, payload.day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return result
