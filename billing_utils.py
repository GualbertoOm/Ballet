# app/services/billing_utils.py
from __future__ import annotations
import json, re
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from typing import Any, Dict, Iterable, Optional

# ---------- Redondeo consistente a 2 decimales ----------
def money(value: Any) -> float:
    """
    Redondeo “contable” (half-up) a 2 decimales. Acepta float/str/Decimal/None.
    """
    if value is None:
        return 0.0
    try:
        q = Decimal(str(value))
    except Exception:
        q = Decimal("0")
    return float(q.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

# ---------- Utilidades de método de pago ----------
_METHOD_ALIASES = {
    "cash": "efectivo",
    "spei": "transferencia",
    "mercado pago": "tarjeta",
    "mercadopago": "tarjeta",
    "tarjeta de crédito": "tarjeta",
    "tarjeta de debito": "tarjeta",
    "tarjeta de débito": "tarjeta",
    "debito": "tarjeta",
    "débito": "tarjeta",
    "transferencia bancaria": "transferencia",
    "depósito": "deposito",
    "deposito bancario": "deposito",
}

def normalize_method(val: str) -> str:
    v = (val or "").strip().lower()
    return _METHOD_ALIASES.get(v, v)

def requires_reference(method_norm: str) -> bool:
    return (method_norm or "") in {"transferencia", "tarjeta", "deposito"}

# ---------- Parseo de condiciones (JSON/CSV/lista) ----------
def parse_conditions(raw: Any) -> list[str]:
    """
    Acepta:
      - JSON: '["efectivo","tarjeta"]'
      - CSV:  "efectivo, tarjeta"
      - Lista/tupla: ["efectivo", "tarjeta"]
    Devuelve lista en minúsculas, sin vacíos.
    """
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(c).strip().lower() for c in raw if str(c).strip()]
    s = str(raw).strip()
    if not s:
        return []
    try:
        data = json.loads(s)
        if isinstance(data, (list, tuple)):
            return [str(c).strip().lower() for c in data if str(c).strip()]
        if isinstance(data, str):
            s = data
    except Exception:
        pass
    parts = re.split(r'[,;|]+', s)
    return [p.strip().lower() for p in parts if p.strip()]

# ---------- Helper de fechas ----------
def _as_date_or_none(d: Any) -> Optional[date]:
    if not d:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    try:
        # ISO 8601 string
        return datetime.fromisoformat(str(d)).date()
    except Exception:
        return None

# ---------- Cálculo de NETO (FULL) ----------
def compute_full_net(
    unit_price: float,
    qty: int,
    *,
    discount_pct: float = 0.0,
    discount_methods: Optional[Iterable[str]] = None,
    discount_valid_until: Optional[Any] = None,   # date|datetime|iso str|None
    surcharge_pct: float = 0.0,
    surcharge_day_cut: int = 0,
    method_norm: str = "",
    today: Optional[date] = None,
    surcharge_on: str = "post_discount",          # "post_discount" | "subtotal"
) -> Dict[str, Any]:
    """
    Reglas:
      - Descuento si: pct>0 AND (condiciones vacío OR method ∈ condiciones) AND (hoy <= vigencia)
      - Recargo  si: pct>0 AND dia_corte>0 AND (hoy.day > dia_corte)
      - Base del recargo: post-descuento (por defecto) o subtotal
    Devuelve dict con subtotal, descuento, recargo, neto y banderas.
    """
    today = today or date.today()
    q = max(int(qty or 1), 1)

    subtotal = money((unit_price or 0.0) * q)

    # --- Descuento ---
    d_methods = [str(m).strip().lower() for m in (discount_methods or [])]
    v_until = _as_date_or_none(discount_valid_until)
    cond_ok = (not d_methods) or ((method_norm or "").lower() in d_methods)
    vig_ok = (v_until is None) or (today <= v_until)
    apply_disc = (float(discount_pct or 0.0) > 0.0) and cond_ok and vig_ok

    disc_amount = money(subtotal * (float(discount_pct or 0.0) / 100.0)) if apply_disc else 0.0
    base_post_disc = max(subtotal - disc_amount, 0.0)

    # --- Recargo ---
    apply_surch = (float(surcharge_pct or 0.0) > 0.0) and int(surcharge_day_cut or 0) > 0 and today.day > int(surcharge_day_cut)
    base_for_surch = subtotal if surcharge_on == "subtotal" else base_post_disc
    surch_amount = money(base_for_surch * (float(surcharge_pct or 0.0) / 100.0)) if apply_surch else 0.0

    neto = money(base_post_disc + surch_amount)

    return {
        "subtotal": subtotal,
        "descuento": disc_amount,
        "recargo": surch_amount,
        "neto": neto,
        "aplico_descuento": bool(apply_disc),
        "aplico_recargo": bool(apply_surch),
        "desc_pct": float(discount_pct or 0.0),
        "recargo_pct": float(surcharge_pct or 0.0),
    }

# ---------- Facade: desde un objeto Pago (tu modelo SQLAlchemy) ----------
def compute_full_net_from_pago(
    pago_obj: Any,
    qty: int,
    method_norm: str,
    today: Optional[date] = None,
    surcharge_on: str = "post_discount",
) -> Dict[str, Any]:
    """
    Extrae campos de tu modelo `Pago` y llama compute_full_net.
    Campos esperados (si alguno no existe, se asume default 0/None):
      - Pago_Monto
      - Pago_Descuento_Porcentaje
      - Pago_Condiciones
      - Pago_Restricciones_Fecha
      - Pago_Recargo_Porcentaje
      - Pago_Recargo_DiaCorte
    """
    unit = getattr(pago_obj, "Pago_Monto", 0.0) or 0.0
    discount_pct = getattr(pago_obj, "Pago_Descuento_Porcentaje", 0.0) or 0.0
    cond_raw = getattr(pago_obj, "Pago_Condiciones", None)
    valid_until = getattr(pago_obj, "Pago_Restricciones_Fecha", None)
    surcharge_pct = getattr(pago_obj, "Pago_Recargo_Porcentaje", 0.0) or 0.0
    surcharge_day = getattr(pago_obj, "Pago_Recargo_DiaCorte", 0) or 0

    return compute_full_net(
        unit_price=float(unit),
        qty=qty,
        discount_pct=float(discount_pct),
        discount_methods=parse_conditions(cond_raw),
        discount_valid_until=valid_until,
        surcharge_pct=float(surcharge_pct),
        surcharge_day_cut=int(surcharge_day),
        method_norm=method_norm,
        today=today,
        surcharge_on=surcharge_on,
    )
