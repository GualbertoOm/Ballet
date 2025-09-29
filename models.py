from extensions import db
from datetime import datetime, date
from enum import Enum
import json
from sqlalchemy import CheckConstraint, text
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import (
    Numeric, Column, Integer, String, Date, DateTime, ForeignKey, Boolean,
    CheckConstraint, Index, func
)



class Tutor(db.Model):
    __tablename__ = 'tutor'
    Tutor_ID = db.Column(db.Integer, primary_key=True)
    Tutor_Nombre = db.Column(db.String(45))
    Tutor_ApellidoP = db.Column(db.String(45))
    Tutor_ApellidoM = db.Column(db.String(45))
    Tutor_Celular = db.Column(db.String(10))
    Tutor_Edad = db.Column(db.Integer)
    Tutor_Parentesco = db.Column(db.String(30))
    Tutor_Correo = db.Column(db.String(100))
    Tutor_Ocupacion = db.Column(db.String(50))
    Tutor_Facebook = db.Column(db.String(100), nullable=True)
    Tutor_Instagram = db.Column(db.String(100), nullable=True)
    Tutor_Direccion = db.Column(db.String(200))
    Tutor_Medio_Entero = db.Column(db.String(100))

    @property
    def facebook_url(self):
        if self.Tutor_Facebook:
            if self.Tutor_Facebook.startswith(('http://', 'https://')):
                return self.Tutor_Facebook
            return f"https://facebook.com/{self.Tutor_Facebook.lstrip('@')}"
        return None
    
    @property
    def instagram_url(self):
        if self.Tutor_Instagram:
            if self.Tutor_Instagram.startswith(('http://', 'https://')):
                return self.Tutor_Instagram
            return f"https://instagram.com/{self.Tutor_Instagram.lstrip('@')}"
        return None


class Instructor(db.Model):
    __tablename__ = 'instructor'
    Instructor_ID = db.Column(db.Integer, primary_key=True)
    Instructor_Nombre = db.Column(db.String(45), nullable=False)
    Instructor_ApellidoP = db.Column(db.String(45), nullable=False)
    Instructor_ApellidoM = db.Column(db.String(45), nullable=True)


class Grupo(db.Model):
    __tablename__ = 'grupo'
    Grupo_ID = db.Column(db.Integer, primary_key=True)
    Grupo_Nombre = db.Column(db.String(50), nullable=False)
    Grupo_Horario = db.Column(db.String(20), nullable=False)
    Grupo_Dias = db.Column(db.String(50), nullable=False)
    Grupo_Nivel = db.Column(db.String(30), nullable=False)
    Instructor_ID = db.Column(db.Integer, db.ForeignKey('instructor.Instructor_ID'))
    instructor = db.relationship('Instructor', backref='grupos')

class Estudiante(db.Model):
    __tablename__ = 'estudiante'
    Est_ID = db.Column(db.Integer, primary_key=True)
    Est_Nombre = db.Column(db.String(45), nullable=False)
    Est_ApellidoP = db.Column(db.String(45), nullable=False)
    Est_ApellidoM = db.Column(db.String(45), nullable=True)
    Est_FechaNac = db.Column(db.Date, nullable=False)
    Est_Sexo = db.Column(db.String(1), nullable=False)  # M/F/O
    Tutor_ID = db.Column(db.Integer, db.ForeignKey('tutor.Tutor_ID'), nullable=False)
    Est_LugarNac = db.Column(db.String(100))
    Est_GradoEscolar = db.Column(db.String(50))
    Est_FechaIngreso = db.Column(db.Date, default=datetime.utcnow)
    # === NUEVO: reingreso ===
    Est_FechaReingreso = db.Column(db.Date, nullable=True)  # si None, se usa Est_FechaIngreso
    Est_Reingreso_Nota = db.Column(db.String(200), nullable=True)  # opcional
    Est_Colegio = db.Column(db.String(100))
    Est_OtrasDisciplinas = db.Column(db.String(200))
    Est_MotivoIngreso = db.Column(db.String(200))
    Est_Status = db.Column(db.String(20), default='Activo')  # Activo, Inactivo, Egresado
    Est_CondicionSalud = db.Column(db.String(200))  # JSON o texto con las condiciones
    Est_Alergias = db.Column(db.String(200))
    Est_Medicamentos = db.Column(db.String(200))
    
    # Relaciones
    tutor = db.relationship('Tutor', backref='estudiantes')
    contactos_emergencia = db.relationship('ContactoEmergencia', backref='estudiante', cascade='all, delete-orphan')
    grupos = db.relationship('Grupo', secondary='estudiante_grupo', backref='estudiantes')


class ContactoEmergencia(db.Model):
    __tablename__ = 'contacto_emergencia'
    Contacto_ID = db.Column(db.Integer, primary_key=True)
    Est_ID = db.Column(db.Integer, db.ForeignKey('estudiante.Est_ID'), nullable=False)
    Contacto_Nombre = db.Column(db.String(45), nullable=False)
    Contacto_ApellidoP = db.Column(db.String(45), nullable=False)
    Contacto_ApellidoM = db.Column(db.String(45), nullable=True)
    Contacto_Telefono = db.Column(db.String(10), nullable=False)
    Contacto_Parentesco = db.Column(db.String(30), nullable=False)


# Tabla de relación muchos a muchos entre Estudiante y Grupo
estudiante_grupo = db.Table('estudiante_grupo',
    db.Column('Est_ID', db.Integer, db.ForeignKey('estudiante.Est_ID'), primary_key=True),
    db.Column('Grupo_ID', db.Integer, db.ForeignKey('grupo.Grupo_ID'), primary_key=True)
)

class Articulo(db.Model):
    __tablename__ = 'articulo'
    Articulo_ID = db.Column(db.Integer, primary_key=True)
    Articulo_Nombre = db.Column(db.String(100), nullable=False)
    Articulo_PrecioVenta = db.Column(Numeric(10, 2), nullable=False)
    Articulo_Existencia = db.Column(db.Integer, nullable=False, default=0)
    Articulo_TipoTalla = db.Column(db.String(20), nullable=True)  # 'talla', 'numero' o None
    Articulo_Tallas = db.Column(db.String(200), nullable=True)  # JSON con tallas y existencias

    def tallas_disponibles(self):
        if self.Articulo_Tallas:
            return json.loads(self.Articulo_Tallas)
        return {}

    def existencia_total(self):
        if self.Articulo_Tallas:
            tallas = json.loads(self.Articulo_Tallas)
            return sum(tallas.values())
        return self.Articulo_Existencia

    def eliminar_talla(self, talla):
        if not self.Articulo_Tallas:
            return False
        tallas = json.loads(self.Articulo_Tallas)
        if talla in tallas:
            del tallas[talla]
            self.Articulo_Tallas = json.dumps(tallas) if tallas else None
            self.Articulo_Existencia = sum(tallas.values()) if tallas else 0
            return True
        return False
    
    @property
    def talla_numero_str(self):
        # Si no tiene tallas definidas
        if not self.Articulo_Tallas:
            return ""
        # Decodifica JSON
        try:
            tallas = json.loads(self.Articulo_Tallas)
            # Solo devuelve las claves (tallas/números) separadas por coma
            return ", ".join(tallas.keys())
        except Exception:
            return ""

    # Relación muchos a muchos con Venta
    ventas = db.relationship('Venta', secondary='venta_articulo', back_populates='articulos')


class Pago(db.Model):
    __tablename__ = 'pago'
    Pago_ID = db.Column(db.Integer, primary_key=True)
    Pago_Monto = db.Column(Numeric(10, 2), nullable=False)
    Pago_Tipo = db.Column(db.String(100), nullable=False)

    # Descuento (ya existentes)
    Pago_Descuento_Tipo = db.Column(db.String(50), nullable=True)
    Pago_Descuento_Porcentaje = db.Column(Numeric(5, 2), nullable=True)
    Pago_Condiciones = db.Column(db.String(200), nullable=True)
    Pago_Restricciones_Fecha = db.Column(Date, nullable=True)

    Pago_Fecha = db.Column(Date, default=date.today)  # o server_default=sa.func.current_date()
    Est_ID = db.Column(db.Integer, db.ForeignKey('estudiante.Est_ID'), nullable=True)

    # Periodicidad (agregado antes)
    Pago_Es_Mensual = db.Column(Boolean, nullable=False, server_default='0')

    # NUEVO: Recargo por pago tardío
    Pago_Tiene_Recargo = db.Column(Boolean, nullable=False, server_default='0')
    Pago_Recargo_Porcentaje = db.Column(Numeric(5, 2), nullable=True)
    # Si es mensual, usamos día del mes; si es único, usamos fecha fija
    Pago_Recargo_DiaMes = db.Column(Integer, nullable=True)  # 1..31 (para mensuales)
    Pago_Recargo_Fecha = db.Column(Date, nullable=True)      # fecha absoluta (para únicos)

    # Expiración (solo aplica a pagos no mensuales)
    Pago_Tiene_Expiracion = db.Column(Boolean, nullable=False, server_default='0')
    Pago_Expira_Fecha     = db.Column(Date, nullable=True)

    estudiante = db.relationship('Estudiante', backref='pagos')
    ventas = db.relationship('Venta', secondary='venta_pago', back_populates='pagos')

    __table_args__ = (
    CheckConstraint('(Pago_Descuento_Porcentaje IS NULL) OR (Pago_Descuento_Porcentaje BETWEEN 0 AND 100)', name='ck_descuento_pct'),
    CheckConstraint('(Pago_Recargo_Porcentaje IS NULL) OR (Pago_Recargo_Porcentaje BETWEEN 0 AND 100)', name='ck_recargo_pct'),
    CheckConstraint('(Pago_Recargo_DiaMes IS NULL) OR (Pago_Recargo_DiaMes BETWEEN 1 AND 31)', name='ck_recargo_dia'),
    )
    __table_args__ = (
    # …los CheckConstraint de arriba (pueden ir en la misma tupla)
    db.Index('ix_pago_tipo', 'Pago_Tipo'),
    db.Index('ix_pago_fecha', 'Pago_Fecha'),
    db.Index('ix_pago_mensual', 'Pago_Es_Mensual'),
    db.Index('ix_pago_expira', 'Pago_Expira_Fecha'),
    )



    def condiciones_lista(self):
        if self.Pago_Condiciones:
            return json.loads(self.Pago_Condiciones)
        return []
    
    def esta_expirado(self, ref: date | None = None) -> bool:
        if self.Pago_Es_Mensual:
            return False
        if not self.Pago_Tiene_Expiracion or not self.Pago_Expira_Fecha:
            return False
        ref = ref or date.today()
        return ref > self.Pago_Expira_Fecha

    # Opcional de conveniencia:
    @property
    def es_mensual(self) -> bool:
        return bool(self.Pago_Es_Mensual)
    


class VentaLinea(db.Model):
    __tablename__ = 'venta_linea'
    Linea_ID = db.Column(db.Integer, primary_key=True)
    Venta_ID = db.Column(db.Integer, db.ForeignKey('venta.Venta_ID'), nullable=False)
    Articulo_ID = db.Column(db.Integer, db.ForeignKey('articulo.Articulo_ID'), nullable=False)
    Talla = db.Column(db.String(50), nullable=True)
    Cantidad = db.Column(db.Integer, nullable=False, default=1)
    Precio_Unitario = db.Column(db.Numeric(10, 2), nullable=False)

    venta = db.relationship('Venta', back_populates='lineas')
    articulo = db.relationship('Articulo')

class Venta(db.Model):
    __tablename__ = 'venta'
    Venta_ID = db.Column(db.Integer, primary_key=True)
    Est_ID = db.Column(db.Integer, db.ForeignKey('estudiante.Est_ID'), nullable=True)
    Instructor_ID = db.Column(db.Integer, db.ForeignKey('instructor.Instructor_ID'), nullable=True)  # Nueva columna
    Metodo_Pago = db.Column(db.String(50))
    Fecha_Venta = db.Column(db.DateTime, nullable=False)
    # NUEVO: número de referencia (transferencia/tarjeta/deposito)
    Referencia_Pago = db.Column(db.String(64), nullable=True)


    # Relaciones
    estudiante = db.relationship('Estudiante', backref='ventas')
    instructor = db.relationship('Instructor', backref='ventas')  # Nueva relación

    # Relaciones muchos a muchos con pagos y articulos:
    pagos = db.relationship('Pago', secondary='venta_pago', back_populates='ventas')
    articulos = db.relationship('Articulo', secondary='venta_articulo', back_populates='ventas')
    lineas = db.relationship('VentaLinea', back_populates='venta', cascade='all, delete-orphan')

    # Validación a nivel de modelo
    __table_args__ = (
        db.CheckConstraint(
            '(Est_ID IS NOT NULL) OR (Instructor_ID IS NOT NULL)',
            name='check_estudiante_or_instructor'
        ),
    )

# Tablas intermedias para las relaciones muchos a muchos:
venta_pago = db.Table('venta_pago',
    db.Column('venta_id', db.Integer, db.ForeignKey('venta.Venta_ID'), primary_key=True),
    db.Column('pago_id', db.Integer, db.ForeignKey('pago.Pago_ID'), primary_key=True)
)

venta_articulo = db.Table('venta_articulo',
    db.Column('venta_id', db.Integer, db.ForeignKey('venta.Venta_ID'), primary_key=True),
    db.Column('articulo_id', db.Integer, db.ForeignKey('articulo.Articulo_ID'), primary_key=True)
)

class Paquete(db.Model):
    __tablename__ = 'paquete'
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), nullable=False, unique=True)
    # 'porcentaje' (0-100), 'monto' (>=0), 'ninguno'
    descuento_tipo = db.Column(db.String(20), nullable=False, default='ninguno')
    descuento_valor = db.Column(db.Numeric(10, 2), nullable=False, default=0)
    activo = db.Column(db.Boolean, nullable=False, default=True)

    # Relación con items
    items = db.relationship('PaqueteItem', back_populates='paquete',
                            cascade='all, delete-orphan')

    __table_args__ = (
        CheckConstraint("descuento_tipo in ('porcentaje','monto','ninguno')", name='ck_paquete_desc_tipo'),
        CheckConstraint("descuento_valor >= 0", name='ck_paquete_desc_valor'),
    )

    @hybrid_property
    def precio_lista(self):
        # Suma de precio actual del artículo * cantidad
        total = 0
        for it in self.items:
            if it.articulo and it.articulo.Articulo_PrecioVenta is not None:
                total += float(it.cantidad) * float(it.articulo.Articulo_PrecioVenta)
        return round(total, 2)

    @hybrid_property
    def precio_descuento(self):
        base = self.precio_lista
        if self.descuento_tipo == 'porcentaje':
            # porcentaje 0-100
            total = base * (1 - float(self.descuento_valor) / 100.0)
        elif self.descuento_tipo == 'monto':
            total = base - float(self.descuento_valor)
        else:
            total = base
        return round(max(total, 0), 2)


class PaqueteItem(db.Model):
    __tablename__ = 'paquete_item'
    id = db.Column(db.Integer, primary_key=True)
    paquete_id = db.Column(db.Integer, db.ForeignKey('paquete.id', ondelete='CASCADE'), nullable=False)
    articulo_id = db.Column(db.Integer, db.ForeignKey('articulo.Articulo_ID'), nullable=False)
    cantidad = db.Column(db.Integer, nullable=False, default=1)

    # NUEVO: aquí guardamos la talla o número elegido (si aplica)
    talla_numero = db.Column(db.String(50), nullable=True)

    paquete = db.relationship('Paquete', back_populates='items')
    articulo = db.relationship('Articulo')

    __table_args__ = (
        CheckConstraint("cantidad > 0", name='ck_paquete_item_cantidad'),
    )


class PlanCobro(db.Model):
    # """
    # Representa un financiamiento/plan abierto para un ítem (artículo, paquete o pago).
    # Un PlanCobro vive para exactamente UN ítem (uno de: Articulo/Paquete/Pago).
    # """
    __tablename__ = 'plan_cobro'

    Plan_ID = Column(Integer, primary_key=True)

    # Titular del plan
    Est_ID = Column(Integer, ForeignKey('estudiante.Est_ID'), nullable=False)
    estudiante = db.relationship('Estudiante', backref='planes_cobro')

    # Ítem financiado (exactamente uno de estos tres debe estar NO NULO)
    Articulo_ID = Column(Integer, ForeignKey('articulo.Articulo_ID'), nullable=True)
    Paquete_ID  = Column(Integer, ForeignKey('paquete.id'), nullable=True)
    Pago_ID     = Column(Integer, ForeignKey('pago.Pago_ID'), nullable=True)

    articulo = db.relationship('Articulo')     # lectura simple
    paquete  = db.relationship('Paquete')      # lectura simple
    pago     = db.relationship('Pago')         # lectura simple

    # Foto/snapshot del precio/base al crear el plan (para reglas claras)
    Precio_Base_Snapshot = Column(Numeric(10, 2), nullable=False)

    # Texto amigable para UI (e.g. "Colegiatura 2025-09", "Uniforme Talla M")
    Descripcion_Resumen = Column(String(200), nullable=False)

    # Montos
    Monto_Total_Original = Column(Numeric(10, 2), nullable=False)
    Saldo_Actual         = Column(Numeric(10, 2), nullable=False)

    # Estado: abierto/liquidado/cancelado
    Estado = Column(String(15), nullable=False, default='abierto')

    # Política de ajuste SOLO al liquidar
    Aplica_Desc_Al_Liquidar = Column(Boolean, nullable=False, server_default='1')
    Vigencia_Inicio = Column(Date, nullable=True)  # para descuento
    Vigencia_Fin    = Column(Date, nullable=True)  # para descuento
    Porc_Descuento  = Column(Numeric(5, 2), nullable=True)  # 0..100
    Monto_Desc_Max  = Column(Numeric(10, 2), nullable=True) # opcional

    Porc_Recargo    = Column(Numeric(5, 2), nullable=True)  # 0..100
    Monto_Rec_Fijo  = Column(Numeric(10, 2), nullable=True) # opcional

    # Logística de entrega (útil para artículos/paquetes)
    Entregable = Column(Boolean, nullable=False, server_default='0')
    Entregado  = Column(Boolean, nullable=False, server_default='0')
    Fecha_Entrega = Column(DateTime, nullable=True)
    Entregado_Por = Column(String(60), nullable=True)

    # Auditoría
    Fecha_Creacion      = Column(DateTime, nullable=False, server_default=func.now())
    Fecha_Ultimo_Abono  = Column(DateTime, nullable=True)
    ReservaStock_Hasta  = Column(Date, nullable=True)  # si decides reservar

    # Relaciones hijas
    abonos = db.relationship(
        'Abono',
        back_populates='plan',
        cascade='all, delete-orphan',
        order_by='Abono.Fecha_Abono'
    )
    liquidacion = db.relationship('Liquidacion', back_populates='plan',
                                  uselist=False, cascade='all, delete-orphan')

    # Conveniencias
    @hybrid_property
    def esta_abierto(self) -> bool:
        return (self.Estado or '').lower() == 'abierto'

    @hybrid_property
    def porcentaje_cubierto(self) -> float:
        try:
            base = float(self.Monto_Total_Original or 0)
            saldo = float(self.Saldo_Actual or 0)
            if base <= 0:
                return 100.0
            pagado = max(0.0, base - saldo)
            return round(100.0 * pagado / base, 2)
        except Exception:
            return 0.0

    @hybrid_property
    def tipo_item(self) -> str:
        """
        Devuelve 'articulo' | 'paquete' | 'pago' según cuál FK esté poblada.
        """
        if self.Articulo_ID is not None:
            return 'articulo'
        if self.Paquete_ID is not None:
            return 'paquete'
        if self.Pago_ID is not None:
            return 'pago'
        return 'desconocido'

    __table_args__ = (
        # Asegurar que SOLO uno de los 3 campos de ítem esté poblado
        CheckConstraint(
            "((Articulo_ID IS NOT NULL) + (Paquete_ID IS NOT NULL) + (Pago_ID IS NOT NULL)) = 1",
            name='ck_plan_un_solo_item'
        ),
        CheckConstraint("Monto_Total_Original >= 0", name='ck_plan_total_no_neg'),
        CheckConstraint("Saldo_Actual >= 0", name='ck_plan_saldo_no_neg'),
        CheckConstraint("(Porc_Descuento IS NULL) OR (Porc_Descuento BETWEEN 0 AND 100)", name='ck_plan_desc_0_100'),
        CheckConstraint("(Porc_Recargo IS NULL) OR (Porc_Recargo BETWEEN 0 AND 100)", name='ck_plan_rec_0_100'),
        Index('ix_plan_est_estado', 'Est_ID', 'Estado'),
        Index('ix_plan_item_art', 'Articulo_ID'),
        Index('ix_plan_item_paq', 'Paquete_ID'),
        Index('ix_plan_item_pag', 'Pago_ID'),
    )


class Abono(db.Model):
    """
    Movimiento de abono contra un PlanCobro. Se liga a una Venta para trazabilidad.
    Guarda además los saldos antes y después para consultas rápidas.
    """
    __tablename__ = 'abono'

    Abono_ID = Column(Integer, primary_key=True)
    Plan_ID  = Column(Integer, ForeignKey('plan_cobro.Plan_ID', ondelete='CASCADE'), nullable=False)
    Venta_ID = Column(Integer, ForeignKey('venta.Venta_ID'), nullable=False)

    # Monto del movimiento (siempre > 0)
    Monto_Abonado = Column(Numeric(10, 2), nullable=False)

    # Saldos alrededor del movimiento (útil para reportes y auditoría)
    # Si estás migrando, mantenlos nullable=True inicialmente
    Saldo_Antes   = Column(Numeric(10, 2), nullable=True)
    Saldo_Despues = Column(Numeric(10, 2), nullable=True)

    Fecha_Abono   = Column(DateTime, nullable=False, server_default=func.now())

    # Copiamos método y referencia de la venta para consultas rápidas (denormalización útil)
    Metodo_Pago      = Column(String(50), nullable=True)
    Referencia_Pago  = Column(String(64), nullable=True)

    Observaciones = Column(String(200), nullable=True)

    plan  = db.relationship('PlanCobro', back_populates='abonos')
    venta = db.relationship('Venta')  # lectura simple

    __table_args__ = (
        CheckConstraint("Monto_Abonado > 0", name='ck_abono_monto_pos'),
        # Checks opcionales (seguros para reportes):
        CheckConstraint("(Saldo_Antes   IS NULL) OR (Saldo_Antes   >= 0)", name='ck_abono_saldo_antes_no_neg'),
        CheckConstraint("(Saldo_Despues IS NULL) OR (Saldo_Despues >= 0)", name='ck_abono_saldo_desp_no_neg'),
        CheckConstraint("""
            (Saldo_Antes   IS NULL) OR
            (Saldo_Despues IS NULL) OR
            (Saldo_Antes >= Saldo_Despues)
        """, name='ck_abono_saldos_consistentes'),
        Index('ix_abono_plan', 'Plan_ID'),
        Index('ix_abono_venta', 'Venta_ID'),
        Index('ix_abono_fecha', 'Fecha_Abono'),
        # Útil para listados por plan ordenados por fecha:
        Index('ix_abono_plan_fecha', 'Plan_ID', 'Fecha_Abono'),
    )



class Liquidacion(db.Model):
    """
    Registro único que aparece cuando un PlanCobro llega a saldo 0.
    Aquí se asientan los ajustes (descuento o recargo) y la venta donde se aplicaron.
    """
    __tablename__ = 'liquidacion'

    Liquidacion_ID = Column(Integer, primary_key=True)
    Plan_ID        = Column(Integer, ForeignKey('plan_cobro.Plan_ID', ondelete='CASCADE'), nullable=False, unique=True)
    Venta_Final_ID = Column(Integer, ForeignKey('venta.Venta_ID'), nullable=False)

    Fecha_Liquidacion = Column(DateTime, nullable=False, server_default=func.now())

    # Ajustes (uno u otro; si ambos son cero, equivale a “sin ajuste”)
    Descuento_Aplicado = Column(Numeric(10, 2), nullable=False, default=0)
    Recargo_Aplicado   = Column(Numeric(10, 2), nullable=False, default=0)

    # Documentación de cómo se calculó
    Base_Calculo = Column(String(20), nullable=False, default='total_original')  # 'total_original' (recomendado)
    Nota_Reglas  = Column(String(200), nullable=True)

    plan         = db.relationship('PlanCobro', back_populates='liquidacion')
    venta_final  = db.relationship('Venta')  # lectura simple

    __table_args__ = (
        CheckConstraint("Descuento_Aplicado >= 0", name='ck_liq_desc_no_neg'),
        CheckConstraint("Recargo_Aplicado >= 0", name='ck_liq_rec_no_neg'),
        Index('ix_liq_plan', 'Plan_ID'),
        Index('ix_liq_venta_final', 'Venta_Final_ID'),
        Index('ix_liq_fecha', 'Fecha_Liquidacion'),
    )
