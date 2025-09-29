from flask_wtf import FlaskForm
from wtforms import (
    StringField, IntegerField, TextAreaField, SelectField, SelectMultipleField,
    DateField, FieldList, FormField, BooleanField, DecimalField, SubmitField, HiddenField
)
from wtforms.validators import(
     DataRequired, Length, Email, Optional, Regexp, NumberRange, ValidationError, AnyOf
)
from datetime import datetime
from flask import flash
from models import (
    Estudiante, Instructor, Tutor, Grupo, Venta, VentaLinea, PaqueteItem,Paquete,Articulo,
    PlanCobro, Abono, Liquidacion 
    )
from extensions import db
from wtforms_sqlalchemy.fields import QuerySelectField
from flask_migrate import Migrate
from datetime import date


# ------------------------------
# Formulario Tutor
# ------------------------------
class TutorForm(FlaskForm):
    # Si usas este select en alguna pantalla (p. ej. para elegir un tutor ya existente),
    # mantenlo. Si no lo usas en ninguna vista, puedes quitarlo del formulario.
    tutor_id = SelectField(
        'Tutor',
        coerce=int,
        validators=[Optional()],
        validate_choice=False  # evita fallo si el valor no está en choices o viene vacío
    )

    #class Meta:
     #   csrf = False  # si usas CSRF global por página, puedes desactivar aquí

    nombre = StringField('Nombre', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(min=2, max=45)
    ])
    apellido_paterno = StringField('Apellido Paterno', validators=[
        DataRequired(), Length(min=2, max=45)
    ])
    apellido_materno = StringField('Apellido Materno', validators=[
        Optional(), Length(max=45)
    ])
    celular = StringField('Celular', validators=[
        DataRequired(), Length(min=10, max=10),
        Regexp('^[0-9]*$', message='Solo números')
    ])
    edad = IntegerField('Edad', validators=[
        DataRequired(), NumberRange(min=18, max=99)
    ])
    # Parentesco lo dejas como texto si lo capturas libre;
    # si prefieres opciones fijas, cámbialo a SelectField con choices.
    parentesco = StringField('Parentesco', validators=[
        DataRequired(), Length(max=30)
    ])
    correo = StringField('Correo Electrónico', validators=[
        Optional(), Email(message='Correo inválido'), Length(max=100)
    ])
    ocupacion = StringField('Ocupación', validators=[
        Optional(), Length(max=50)
    ])
    facebook = StringField('Facebook', validators=[
        Optional(), Length(max=100),
        Regexp(
            r'^(https?://)?(www\.)?(facebook\.com/)?@?[a-zA-Z0-9_\.]+$',
            message='Ingresa tu usuario (ej: @usuario) o enlace completo'
        )
    ])
    instagram = StringField('Instagram', validators=[
        Optional(), Length(max=100),
        Regexp(
            r'^(https?://)?(www\.)?(instagram\.com/)?@?[a-zA-Z0-9_\.]+$',
            message='Ingresa tu usuario (ej: @usuario) o enlace completo'
        )
    ])
    direccion = TextAreaField('Dirección', validators=[
        Optional(), Length(max=200)
    ])
    medio_entero = SelectField('¿Cómo se enteró?', choices=[
        ('', 'Seleccione...'),
        ('Redes sociales', 'Redes sociales'),
        ('Recomendación', 'Recomendación'),
        ('Volante', 'Volante'),
        ('Página web', 'Página web'),
        ('Otro', 'Otro')
    ], validators=[Optional()], validate_choice=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # WTForms 3 NO tolera choices=None en SelectField.
        # Aseguramos una lista (aunque sea vacía) y, si quieres, la lista real de tutores:
        try:
            self.tutor_id.choices = [
                (t.Tutor_ID, f"{t.Tutor_Nombre} {t.Tutor_ApellidoP}")
                for t in Tutor.query.order_by(Tutor.Tutor_Nombre).all()
            ]
        except Exception:
            # Si por alguna razón no puedes consultar la DB aquí,
            # al menos evita None:
            self.tutor_id.choices = []

        # Por coherencia, garantizamos que medio_entero siempre tenga lista:
        if self.medio_entero.choices is None:
            self.medio_entero.choices = []


# ------------------------------
# Formulario Instructor
# ------------------------------
class InstructorForm(FlaskForm):
    nombre = StringField('Nombre', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(min=2, max=45)
    ])
    apellido_paterno = StringField('Apellido Paterno', validators=[
        DataRequired(), Length(min=2, max=45)
    ])
    apellido_materno = StringField('Apellido Materno', validators=[
        Optional(), Length(max=45)
    ])

# ------------------------------
# Formulario Grupo
# ------------------------------
class GrupoForm(FlaskForm):
    nombre = StringField('Nombre del Grupo', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(min=2, max=50)
    ])
    horario = StringField('Horario', validators=[
        DataRequired(), Length(max=20)
    ])
    dias = StringField('Días', validators=[
        DataRequired(), Length(max=50)
    ])
    nivel = SelectField('Nivel', choices=[
        ('Principiante', 'Principiante'),
        ('Intermedio', 'Intermedio'),
        ('Avanzado', 'Avanzado')
    ], validators=[DataRequired()])
    instructor_id = SelectField('Instructor', coerce=int, validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        super(GrupoForm, self).__init__(*args, **kwargs)
        self.instructor_id.choices = [
            (i.Instructor_ID, f"{i.Instructor_Nombre} {i.Instructor_ApellidoP}")
            for i in Instructor.query.order_by(Instructor.Instructor_Nombre).all()
        ]

# ------------------------------
# Formulario Contacto Emergencia
# ------------------------------
class ContactoEmergenciaForm(FlaskForm):
    class Meta:
        csrf = False  # Para usar como subform sin csrf

    nombre = StringField('Nombre', validators=[
        Optional(), Length(min=2, max=45)
    ])
    apellido_paterno = StringField('Apellido Paterno', validators=[
        Optional(), Length(min=2, max=45)
    ])
    apellido_materno = StringField('Apellido Materno', validators=[
        Optional(), Length(max=45)
    ])
    telefono = StringField('Teléfono', validators=[
        Optional(), Length(min=10, max=10),
        Regexp('^[0-9]*$', message='Solo números')
    ])
    parentesco = StringField('Parentesco', validators=[
        Optional(), Length(max=30)
    ])

# ------------------------------
# Formulario Estudiante
# ------------------------------
class EstudianteForm(FlaskForm):
    class Meta:
        csrf = False
    nombre = StringField('Nombre', validators=[
        DataRequired(message="El nombre es obligatorio"),
        Length(min=2, max=45)
    ])
    apellido_paterno = StringField('Apellido Paterno', validators=[
        DataRequired(), Length(min=2, max=45)
    ])
    apellido_materno = StringField('Apellido Materno', validators=[
        Optional(), Length(max=45)
    ])
    fecha_nacimiento = DateField('Fecha de Nacimiento', format='%Y-%m-%d', validators=[DataRequired()], default=datetime.utcnow)
    sexo = SelectField('Sexo', choices=[
        ('M', 'Masculino'),
        ('F', 'Femenino'),
        ('O', 'Otro')
    ], validators=[DataRequired()])
    tutor_id = SelectField('Tutor', coerce=int, validators=[DataRequired()])
    lugar_nacimiento = StringField('Lugar de Nacimiento', validators=[Optional(), Length(max=100)])
    grado_escolar = StringField('Grado Escolar', validators=[Optional(), Length(max=50)])
    fecha_ingreso = DateField('Fecha de Ingreso', format='%Y-%m-%d', default=datetime.utcnow)
    colegio = StringField('Colegio', validators=[Optional(), Length(max=100)])
    # === NUEVO: reingreso ===
    marcar_reingreso = BooleanField('Marcar reingreso', default=False)
    fecha_reingreso  = DateField('Fecha de reingreso', format='%Y-%m-%d', validators=[Optional()], default=datetime.utcnow)
    nota_reingreso   = StringField('Nota de reingreso', validators=[Optional(), Length(max=200)])

    # Grupos como checkboxes
    grupos = FieldList(BooleanField('Grupo'), min_entries=0)

    otras_disciplinas = TextAreaField('Otras Disciplinas', validators=[Optional(), Length(max=200)])
    motivo_ingreso = TextAreaField('Motivo de Ingreso', validators=[Optional(), Length(max=200)])
    status = SelectField('Status', choices=[
        ('Activo', 'Activo'),
        ('Inactivo', 'Inactivo'),
        ('Egresado', 'Egresado')
    ], default='Activo')

    # Condiciones de salud guardadas como JSON en texto
    pie_plano = BooleanField('Pie Plano', default=False)
    escoliosis = BooleanField('Escoliosis', default=False)
    genu_varo = BooleanField('Genu Varo', default=False)
    genu_valgo = BooleanField('Genu Valgo', default=False)
    desviacion_cadera = BooleanField('Desviación de Cadera', default=False)
    asma = BooleanField('Asma', default=False)
    psicopatologias = BooleanField('Psicopatologías', default=False)
    otras_condiciones = StringField('Otras Condiciones', validators=[Optional(), Length(max=100)])

    alergias = TextAreaField('Alergias', validators=[Optional(), Length(max=200)])
    medicamentos = TextAreaField('Medicamentos', validators=[Optional(), Length(max=200)])

    contacto_principal = FormField(ContactoEmergenciaForm)
    contacto_secundario = FormField(ContactoEmergenciaForm)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cargar tutores
        self.tutor_id.choices = [(t.Tutor_ID, f"{t.Tutor_Nombre} {t.Tutor_ApellidoP}") for t in Tutor.query.order_by(Tutor.Tutor_Nombre).all()]

        # Cargar grupos para checkboxes
        self.grupos_disponibles = Grupo.query.order_by(Grupo.Grupo_Nombre).all()
        while len(self.grupos.entries) < len(self.grupos_disponibles):
            self.grupos.append_entry()

    def validate(self, extra_validators=None):
        if not super().validate(extra_validators=extra_validators):
            return False

        # Validar contacto principal completo
        cp = self.contacto_principal
        if not (cp.nombre.data and cp.apellido_paterno.data and cp.telefono.data and cp.parentesco.data):
            cp.nombre.errors.append('Todos los campos del contacto principal son requeridos')
            return False

        return True

# ------------------------------
# Formulario para tallas/números en artículos
# ------------------------------
class TallaNumeroForm(FlaskForm):
    nombre = StringField('Talla/Número', validators=[DataRequired()])
    cantidad = IntegerField('Cantidad', validators=[DataRequired(), NumberRange(min=1)])

# ------------------------------
# Formulario Artículo
# ------------------------------
class ArticuloForm(FlaskForm):
    nombre = StringField('Nombre del Artículo', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(min=2, max=100)
    ])
    precio = DecimalField('Precio de Venta', validators=[
        DataRequired(message='El precio es requerido'),
        NumberRange(min=0.01, message='El precio debe ser mayor a 0')
    ], places=2)
    existencia = IntegerField('Existencia General', validators=[
        NumberRange(min=0, message='La existencia no puede ser negativa'),
        Optional()
    ], default=0)
    tipo_talla = SelectField('Tipo de Talla/Número', choices=[
        ('ninguno', 'No aplica'),
        ('talla', 'Talla (CH, M, G, etc.)'),
        ('numero', 'Número (1-3, 3-5, etc.)')
    ], default='ninguno')
    tallas_numeros = FieldList(FormField(TallaNumeroForm), min_entries=0, label='Tallas/Números')

    def validate(self, extra_validators=None):
        if not super().validate(extra_validators=extra_validators):
            return False

        if self.tipo_talla.data != 'ninguno':
            if not self.tallas_numeros.data:
                self.tipo_talla.errors.append('Debe agregar al menos una talla/número.')
                return False
            suma = sum(int(item['cantidad']) for item in self.tallas_numeros.data)
            if self.existencia.data != 0:
                self.existencia.errors.append(
                    'Para artículos con tallas/números, la existencia general debe ser 0.'
                    ' Las existencias se manejan por talla/número.'
                )
                return False
        else:
            if self.tallas_numeros.data:
                self.tipo_talla.errors.append(
                    'No puede agregar tallas/números para artículos que no usan tallas/números.'
                )
                return False
            if self.existencia.data <= 0:
                self.existencia.errors.append(
                    'Para artículos sin tallas/números, la existencia general debe ser mayor a 0.'
                )
                return False

        return True

# ------------------------------
# Formulario Pago
# ------------------------------
class PagoForm(FlaskForm):
    tipo_pago = StringField('Tipo de Pago', validators=[
        DataRequired(message='⚠️ El tipo de pago es obligatorio'),
        Length(min=2, max=100)
    ])
    monto = DecimalField('Monto $', validators=[
        DataRequired(message='⚠️ El monto es obligatorio'),
        NumberRange(min=0.01)
    ], places=2)

    # Periodicidad
    es_mensual = BooleanField('Es mensual (recurrente)', default=False)

    # Descuento
    aplicar_descuento = BooleanField('Aplicar descuento', default=False)
    nombre_descuento = StringField('Nombre del Descuento', validators=[Optional(), Length(max=50)])
    porcentaje_descuento = DecimalField('Porcentaje de Descuento %', places=2,
                                        validators=[Optional(), NumberRange(min=0, max=100)])
    condicion_efectivo = BooleanField('Efectivo', default=False)
    condicion_tarjeta = BooleanField('Tarjeta', default=False)
    condicion_transferencia = BooleanField('Transferencia', default=False)
    condicion_deposito = BooleanField('Depósito', default=False)
    restricciones_fecha = DateField('Válido hasta', format='%Y-%m-%d', validators=[Optional()])

    # NUEVO: Recargo
    aplicar_recargo = BooleanField('Aplicar recargo por pago tardío', default=False)
    porcentaje_recargo = DecimalField('Porcentaje de recargo %', places=2,
                                      validators=[Optional(), NumberRange(min=0, max=100)])
    recargo_dia_mes = IntegerField('Día del mes para aplicar recargo (1–31)', validators=[Optional(), NumberRange(min=1, max=31)])
    recargo_fecha = DateField('Aplicar recargo a partir de (fecha)', format='%Y-%m-%d', validators=[Optional()])

    # NUEVO: Expiración para pagos únicos
    aplicar_expiracion = BooleanField('Tiene fecha de expiración', default=False)
    expira_fecha = DateField('Expira el', format='%Y-%m-%d', validators=[Optional()])

    def validate(self, extra_validators=None):
        if not super().validate(extra_validators=extra_validators):
            return False

        # --- REGLA DE EXPIRACIÓN vs MENSUAL ---
        # Los pagos mensuales NO pueden tener expiración
        if self.es_mensual.data and getattr(self, "aplicar_expiracion", None) and self.aplicar_expiracion.data:
            self.aplicar_expiracion.errors.append('Los pagos mensuales no pueden tener expiración.')
            return False

        # Si NO es mensual y se marcó expiración, la fecha es obligatoria
        if (not self.es_mensual.data) and getattr(self, "aplicar_expiracion", None) and self.aplicar_expiracion.data:
            if not self.expira_fecha.data:
                self.expira_fecha.errors.append('⚠️ La fecha de expiración es obligatoria para pagos únicos con expiración.')
                return False

        # --- Validación de descuento (igual que dejamos antes) ---
        if self.aplicar_descuento.data:
            if not self.nombre_descuento.data:
                self.nombre_descuento.errors.append('⚠️ Nombre del descuento es requerido')
                return False
            if self.porcentaje_descuento.data is None:
                self.porcentaje_descuento.errors.append('⚠️ Porcentaje requerido')
                return False
            if not any([self.condicion_efectivo.data,
                        self.condicion_tarjeta.data,
                        self.condicion_transferencia.data,
                        self.condicion_deposito.data]):
                flash('⚠️ Seleccione al menos una condición de pago', 'warning')
                return False
            # Fecha límite solo si NO es mensual
            if not self.es_mensual.data and not self.restricciones_fecha.data:
                self.restricciones_fecha.errors.append('⚠️ Fecha límite requerida para descuentos en pagos no mensuales')
                return False

        # --- NUEVO: Validación de recargo ---
        if self.aplicar_recargo.data:
            if self.porcentaje_recargo.data is None:
                self.porcentaje_recargo.errors.append('⚠️ Porcentaje de recargo requerido')
                return False

            if self.es_mensual.data:
                # Mensual → requerimos día del mes, NO fecha
                if self.recargo_dia_mes.data is None:
                    self.recargo_dia_mes.errors.append('⚠️ Indique el día del mes (1–31) a partir del cual aplica recargo')
                    return False
                # si viene fecha por error, la ignoraremos en la ruta
            else:
                # Único → requerimos fecha fija, NO día del mes
                if not self.recargo_fecha.data:
                    self.recargo_fecha.errors.append('⚠️ Indique la fecha a partir de la cual aplica recargo')
                    return False
                # si viene día por error, lo ignoraremos en la ruta
  
        # --- NUEVO: Validación de expiración ---
        if self.aplicar_expiracion.data:
            if self.es_mensual.data:
                # Si es mensual, ignoraremos la expiración (no aplica)
                pass
            else:
                if not self.expira_fecha.data:
                    self.expira_fecha.errors.append('⚠️ Indique la fecha de expiración para pagos únicos')
                    return False

        return True

# IMPORTANTE: agrega AnyOf a tus imports de validadores
# from wtforms.validators import DataRequired, Length, Email, Optional, Regexp, NumberRange, ValidationError
# cámbialo por:
# from wtforms.validators import DataRequired, Length, Email, Optional, Regexp, NumberRange, ValidationError, AnyOf
# (si ya lo tienes, ignora este comentario)


class AbonoLineaForm(FlaskForm):
    class Meta:
        csrf = False  # Va dentro de VentaForm (que sí tiene CSRF)

    # Usar plan existente (si lo hay)
    plan_id = IntegerField('Plan_ID', validators=[Optional()])

    # Si no hay plan, permitir que el backend cree uno (buscar/crear)
    crear_plan_si_no_existe = BooleanField('Crear plan si no existe', default=True)

    # Identidad del ítem (requerido si no viene plan_id)
    tipo_item = SelectField(
        'Tipo de ítem',
        choices=[('articulo', 'Artículo'), ('paquete', 'Paquete'), ('pago', 'Pago')],
        validators=[Optional(), AnyOf(['articulo', 'paquete', 'pago'])]
    )
    item_ref_id = IntegerField('Item Ref ID', validators=[Optional()])  # Articulo_ID / Paquete_ID / Pago_ID

    # Snapshot/UI (útil si el backend crea plan nuevo)
    descripcion_resumen = HiddenField(validators=[Optional()])
    monto_total_original = DecimalField('Total original', places=2, validators=[Optional(), NumberRange(min=0)])

    # Movimiento
    monto_abono = DecimalField('Monto a abonar', places=2, validators=[Optional(), NumberRange(min=0.01)])
    liquidar = BooleanField('Liquidar (saldo total)', default=False)

    observaciones = StringField('Observaciones', validators=[Optional(), Length(max=200)])

    def validate(self, extra_validators=None):
        ok = super().validate(extra_validators=extra_validators)
        if not ok:
            return False

        # Debe venir plan_id O (tipo_item + item_ref_id)
        if not self.plan_id.data:
            if not self.tipo_item.data or not self.item_ref_id.data:
                self.item_ref_id.errors.append('Falta identificar el ítem (tipo_item + item_ref_id) o indicar un plan existente.')
                return False

        # Debe venir monto_abono > 0 o liquidar=True
        if not self.liquidar.data:
            if not self.monto_abono.data or float(self.monto_abono.data) <= 0:
                self.monto_abono.errors.append('Indica un monto (> 0) o marca "Liquidar".')
                return False

        return True

# ------------------------------
# Ventas
# ------------------------------
# === Tu VentaForm con la ampliación mínima (FieldList abonos) ===
class VentaForm(FlaskForm):
    # Lo setea el template via JS: "estudiante" o "instructor"
    tipo_cliente = HiddenField('Tipo de Cliente', validators=[Optional()])

    estudiante_id = SelectField('Estudiante', coerce=int, validators=[Optional()], default=0)
    instructor_id = SelectField('Instructor', coerce=int, validators=[Optional()], default=0)

    metodo_pago = SelectField(
        'Método de Pago',
        choices=[('efectivo', 'Efectivo'), ('tarjeta', 'Tarjeta'),
                 ('transferencia', 'Transferencia'), ('deposito', 'Déposito')],
        validators=[DataRequired()]
    )

    referencia_pago = StringField(
        'Referencia',
        validators=[Optional(), Length(max=64, message="Máx 64 caracteres")]
    )

    # Opción A: que no sea obligatoria si la ruta no la usa
    fecha_venta = DateField('Fecha de Venta', format='%Y-%m-%d',
                            validators=[Optional()], default=date.today)

    pagos = SelectMultipleField('Pagos', coerce=int, validators=[Optional()])

    # CLAVE: usamos str porque las claves de variantes pueden ser "ID" o "ID:::TALLA"
    articulos = SelectMultipleField('Artículos', coerce=str, validators=[Optional()])

    # >>> NUEVO: aquí el modal “empuja” abonos/liquidaciones a procesar en el POST
    abonos = FieldList(FormField(AbonoLineaForm), min_entries=0)

    submit = SubmitField('Registrar Venta')

    def validate(self, extra_validators=None):
        ok = super().validate(extra_validators=extra_validators)
        if not ok:
            return False
        if not self.estudiante_id.data and not self.instructor_id.data:
            self.estudiante_id.errors.append("Debe seleccionar un estudiante o un instructor")
            return False
        # Los abonos se validan dentro de cada AbonoLineaForm
        return True

def articulos_query():
    return Articulo.query.order_by(Articulo.Articulo_Nombre.asc())

class PaqueteItemForm(FlaskForm):
    articulo = QuerySelectField('Artículo', query_factory=articulos_query,
                                allow_blank=False, get_label='Articulo_Nombre')
    cantidad = IntegerField('Cantidad', default=1, validators=[DataRequired(), NumberRange(min=1)])

class PaqueteForm(FlaskForm):
    nombre = StringField('Nombre del Paquete', validators=[DataRequired(), Length(min=2, max=120)])
    descuento_tipo = SelectField('Tipo de descuento',
                                 choices=[('ninguno', 'Ninguno'),
                                          ('porcentaje', 'Porcentaje (%)'),
                                          ('monto', 'Monto fijo')],
                                 default='ninguno')
    descuento_valor = DecimalField('Valor de descuento', default=0, validators=[NumberRange(min=0), Optional()])
    items = FieldList(FormField(PaqueteItemForm), min_entries=1)
    submit = SubmitField('Guardar Paquete')

