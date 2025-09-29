# plan_utils.py
from __future__ import annotations
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy import func

# Utilidades del proyecto
from billing_utils import money, requires_reference


# ---------------------------------------------
# Helpers internos
# ---------------------------------------------
def _now() -> datetime:
    return datetime.now()


def _lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _get_models():
    """
    Import lazy para evitar ciclos:
    - extensions.db
    - models.PlanCobro, Abono, Pago, Venta, Liquidacion
    """
    from extensions import db
    from models import PlanCobro, Abono, Pago, Venta, Liquidacion
    return db, PlanCobro, Abono, Pago, Venta, Liquidacion


# ---------------------------------------------
# BÚSQUEDA Y CREACIÓN DE PLANES
# ---------------------------------------------
def find_open_plan(
    est_id: int,
    *,
    pago_id: Optional[int] = None,
    articulo_id: Optional[int] = None,
    paquete_id: Optional[int] = None,
) -> Optional[object]:
    """
    Devuelve el PlanCobro ABIERTO más reciente para el titular/ítem.
    Se espera EXACTAMENTE UNO de (pago_id | articulo_id | paquete_id).
    """
    db, PlanCobro, *_ = _get_models()

    q = PlanCobro.query.filter(
        PlanCobro.Est_ID == est_id,
        func.lower(PlanCobro.Estado) == 'abierto'
    )
    if pago_id is not None:
        q = q.filter(PlanCobro.Pago_ID == pago_id)
    elif articulo_id is not None:
        q = q.filter(PlanCobro.Articulo_ID == articulo_id)
    elif paquete_id is not None:
        q = q.filter(PlanCobro.Paquete_ID == paquete_id)
    else:
        return None

    return q.order_by(PlanCobro.Fecha_Creacion.desc()).first()


def get_or_create_plan(
    est_id: int,
    *,
    pago_obj: Optional[object] = None,
    articulo_obj: Optional[object] = None,
    paquete_obj: Optional[object] = None,
    qty: int = 1,
    descripcion_resumen: Optional[str] = None,
    aplicar_descuento_al_liquidar: bool = True,
) -> Tuple[object, bool]:
    """
    Obtiene un plan ABIERTO del ítem; si no existe, lo crea.
    Retorna (plan, creado_nuevo: bool).

    - Para PAGO/ARTÍCULO/PAQUETE:
      * Precio_Base_Snapshot = precio catálogo a la fecha
      * Monto_Total_Original = qty * precio
      * Saldo_Actual = Monto_Total_Original
      * Descripcion_Resumen = nombre o uno sugerido
      * Aplica_Desc_Al_Liquidar = controla que el ajuste se compute al final
    """
    db, PlanCobro, *_ = _get_models()
    now = _now()
    qty = max(int(qty or 1), 1)

    pago_id = getattr(pago_obj, "Pago_ID", None) if pago_obj else None
    art_id = getattr(articulo_obj, "Articulo_ID", None) if articulo_obj else None
    paq_id = getattr(paquete_obj, "Paquete_ID", None) or getattr(paquete_obj, "id", None) if paquete_obj else None

    existing = find_open_plan(est_id, pago_id=pago_id, articulo_id=art_id, paquete_id=paq_id)
    if existing:
        return existing, False

    if pago_obj:
        unit = float(getattr(pago_obj, "Pago_Monto", 0.0) or 0.0)
        total = money(unit * qty)
        desc = descripcion_resumen or f"{getattr(pago_obj, 'Pago_Tipo', 'Pago')} x{qty}"
        plan = PlanCobro(
            Est_ID=est_id,
            Pago_ID=pago_id,
            Precio_Base_Snapshot=money(unit),
            Descripcion_Resumen=desc,
            Monto_Total_Original=total,
            Saldo_Actual=total,
            Estado='abierto',
            Aplica_Desc_Al_Liquidar=bool(aplicar_descuento_al_liquidar),
            Fecha_Creacion=now,
        )
    elif articulo_obj:
        unit = float(getattr(articulo_obj, "Precio", 0.0) or 0.0)
        total = money(unit * qty)
        desc = descripcion_resumen or f"Artículo #{art_id} x{qty}"
        plan = PlanCobro(
            Est_ID=est_id,
            Articulo_ID=art_id,
            Precio_Base_Snapshot=money(unit),
            Descripcion_Resumen=desc,
            Monto_Total_Original=total,
            Saldo_Actual=total,
            Estado='abierto',
            Aplica_Desc_Al_Liquidar=bool(aplicar_descuento_al_liquidar),
            Fecha_Creacion=now,
        )
    elif paquete_obj:
        unit = float(getattr(paquete_obj, "precio", 0.0) or 0.0)
        total = money(unit * qty)
        desc = descripcion_resumen or f"Paquete #{paq_id} x{qty}"
        plan = PlanCobro(
            Est_ID=est_id,
            Paquete_ID=paq_id,
            Precio_Base_Snapshot=money(unit),
            Descripcion_Resumen=desc,
            Monto_Total_Original=total,
            Saldo_Actual=total,
            Estado='abierto',
            Aplica_Desc_Al_Liquidar=bool(aplicar_descuento_al_liquidar),
            Fecha_Creacion=now,
        )
    else:
        raise ValueError("Debes proporcionar pago_obj, articulo_obj o paquete_obj")

    db.session.add(plan)
    db.session.flush()
    return plan, True


# ---------------------------------------------
# ABONOS y LIQUIDACIÓN
# ---------------------------------------------
def sum_abonos_plan(plan_id: int) -> float:
    """
    Suma Monto_Abonado para el plan (0.00 si no hay).
    """
    db, _, Abono, *_ = _get_models()
    total = (db.session.query(func.coalesce(func.sum(Abono.Monto_Abonado), 0.0))
             .filter(Abono.Plan_ID == plan_id)
             .scalar())
    try:
        return float(total or 0.0)
    except Exception:
        return 0.0


def registrar_abono(
    plan: object,
    venta: object,
    *,
    monto: float,
    metodo_norm: Optional[str] = None,
    referencia: Optional[str] = None,
    observaciones: Optional[str] = None,
    close_if_zero: bool = False,
) -> Optional[object]:
    """
    Inserta un Abono (cap al saldo) y actualiza saldos del plan.
    - Guarda Saldo_Antes / Saldo_Despues
    - Copia Metodo_Pago / Referencia_Pago (si el método la requiere)
    - Actualiza Fecha_Ultimo_Abono
    - Si close_if_zero=True, y Saldo_Despues==0 → el plan queda efectivamente liquidado
    """
    db, _, Abono, *_ = _get_models()

    monto = money(monto)
    if monto <= 0:
        raise ValueError("El monto del abono debe ser > 0")

    saldo_antes = float(plan.Saldo_Actual or 0.0)
    if saldo_antes <= 0:
        # Nada que abonar
        return None

    monto_efectivo = min(money(saldo_antes), monto)
    if monto_efectivo <= 0:
        return None

    saldo_despues = money(saldo_antes - monto_efectivo)

    ab = Abono(
        Plan_ID=plan.Plan_ID,
        Venta_ID=getattr(venta, "Venta_ID", None),
        Monto_Abonado=monto_efectivo,
        Saldo_Antes=money(saldo_antes),
        Saldo_Despues=saldo_despues,
        Fecha_Abono=_now(),
        Metodo_Pago=(metodo_norm or None),
        Referencia_Pago=(referencia if (metodo_norm and requires_reference(metodo_norm)) else None),
        Observaciones=(observaciones or None),
    )
    db.session.add(ab)

    # Actualiza plan
    plan.Saldo_Actual = saldo_despues
    plan.Fecha_Ultimo_Abono = _now()

    # No hay campo Fecha_Cierre en PlanCobro; la "liquidación" formal la registra Liquidacion.
    # Aquí solo podemos dejar el saldo en 0 y Estado='abierto' o lo que uses;
    # tu UI considerará saldo 0 como plan liquidado en la práctica.
    if close_if_zero and saldo_despues <= 0.0:
        plan.Estado = 'abierto'  # se mantiene; Liquidacion dejará constancia formal

    db.session.flush()
    return ab


def liquidar_plan(
    plan: object,
    venta: object,
    *,
    neto_full: float,
    metodo_norm: Optional[str] = None,
    referencia: Optional[str] = None,
    observaciones: str = "Liquidación automática (FULL)",
    nota_reglas: Optional[str] = None,
) -> Optional[object]:
    """
    Liquida un plan abierto:
      1) Calcula delta = max(0, neto_full - abonos_previos).
      2) Abona monto_a_abonar = min(delta, Saldo_Actual). Si delta<=0 pero queda saldo,
         abonamos el saldo (para no dejar planes atorados).
      3) Deja Saldo_Actual=0.
      4) Registra LIQUIDACION con el ajuste aplicado contra Monto_Total_Original:
         - Descuento_Aplicado = max(0, Monto_Total_Original - neto_full)
         - Recargo_Aplicado   = max(0, neto_full - Monto_Total_Original)
         Base_Calculo='total_original', Nota_Reglas=nota_reglas si se provee.

    Devuelve el Abono final realizado (o None si no hubo que abonar).
    """
    db, PlanCobro, Abono, Pago, Venta, Liquidacion = _get_models()

    if _lower(getattr(plan, "Estado", "abierto")) not in ("abierto",):
        # Si ya no está abierto, nada que hacer
        return None

    neto_full = money(neto_full)
    abonos_previos = sum_abonos_plan(plan.Plan_ID)
    delta = money(max(0.0, neto_full - abonos_previos))
    saldo = money(getattr(plan, "Saldo_Actual", 0.0) or 0.0)

    if saldo <= 0.0:
        # Asegura registro de Liquidacion si faltara (inconsistencia)
        _ensure_liquidacion(db, plan, venta, neto_full, nota_reglas)
        db.session.flush()
        return None

    # Si delta es 0 o negativo pero queda saldo (p.ej. por cambio de política),
    # abonamos el saldo para cerrar correctamente.
    monto_a_abonar = delta if delta > 0 else saldo
    monto_a_abonar = min(monto_a_abonar, saldo)

    ab_final = None
    if monto_a_abonar > 0:
        ab_final = registrar_abono(
            plan=plan,
            venta=venta,
            monto=monto_a_abonar,
            metodo_norm=metodo_norm,
            referencia=referencia,
            observaciones=observaciones,
            close_if_zero=True,  # deja saldo en 0
        )

    # Fuerza saldo a 0
    plan.Saldo_Actual = money(0.0)

    # Registrar LIQUIDACIÓN (única por plan)
    _ensure_liquidacion(db, plan, venta, neto_full, nota_reglas)

    db.session.flush()
    return ab_final


def _ensure_liquidacion(db, plan, venta, neto_full: float, nota_reglas: Optional[str]):
    """
    Crea (si no existe) la fila de Liquidacion para el plan:
      - Descuento_Aplicado: Monto_Total_Original - neto_full (si >0)
      - Recargo_Aplicado:   neto_full - Monto_Total_Original (si >0)
      - Base_Calculo: 'total_original'
      - Venta_Final_ID: venta.Venta_ID
    """
    from models import Liquidacion  # acceso directo para type-checkers

    # ¿Ya existe?
    liq = (db.session.query(Liquidacion)
           .filter(Liquidacion.Plan_ID == plan.Plan_ID)
           .one_or_none())
    if liq:
        # Idempotencia: no duplicar
        return liq

    total_original = money(getattr(plan, "Monto_Total_Original", 0.0) or 0.0)
    neto_full = money(neto_full)

    descuento = money(max(0.0, total_original - neto_full))
    recargo   = money(max(0.0, neto_full - total_original))

    liq = Liquidacion(
        Plan_ID=plan.Plan_ID,
        Venta_Final_ID=getattr(venta, "Venta_ID", None),
        Fecha_Liquidacion=_now(),
        Descuento_Aplicado=descuento,
        Recargo_Aplicado=recargo,
        Base_Calculo='total_original',
        Nota_Reglas=(nota_reglas or None),
    )
    db.session.add(liq)
    return liq
