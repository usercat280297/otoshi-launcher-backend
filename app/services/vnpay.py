import hashlib
import hmac
import urllib.parse
from datetime import datetime
from typing import Optional

from ..core.config import VNPAY_API_URL, VNPAY_SECRET_KEY, VNPAY_TMN_CODE


class VNPayService:
    def __init__(self) -> None:
        self.tmn_code = VNPAY_TMN_CODE
        self.secret_key = VNPAY_SECRET_KEY
        self.api_url = VNPAY_API_URL

    def enabled(self) -> bool:
        return bool(self.tmn_code and self.secret_key)

    def create_payment_url(
        self,
        order_id: str,
        amount_vnd: int,
        return_url: str,
        description: str,
    ) -> str:
        if not self.enabled():
            raise RuntimeError("VNPay is not configured")
        params = {
            "vnp_Version": "2.1.0",
            "vnp_Command": "pay",
            "vnp_TmnCode": self.tmn_code,
            "vnp_Amount": amount_vnd * 100,
            "vnp_CurrCode": "VND",
            "vnp_TxnRef": order_id,
            "vnp_OrderInfo": description,
            "vnp_OrderType": "billpayment",
            "vnp_Locale": "vn",
            "vnp_ReturnUrl": return_url,
            "vnp_CreateDate": datetime.now().strftime("%Y%m%d%H%M%S"),
        }

        secure_hash = self._hash_params(params)
        params["vnp_SecureHash"] = secure_hash
        return f"{self.api_url}?{urllib.parse.urlencode(params)}"

    def verify_payment(self, params: dict) -> bool:
        if not self.enabled():
            raise RuntimeError("VNPay is not configured")
        params = dict(params)
        secure_hash = params.pop("vnp_SecureHash", "")
        expected = self._hash_params(params)
        return hmac.compare_digest(secure_hash, expected)

    def _hash_params(self, params: dict) -> str:
        items = sorted(params.items())
        query = "&".join([f"{key}={value}" for key, value in items])
        return hmac.new(
            self.secret_key.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()


vnpay_service = VNPayService()
