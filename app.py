from flask import (
    Flask, render_template, request, redirect, url_for, flash, make_response, current_app, 
    jsonify, session
)
import io, csv, os, uuid, json, re                 # ← AÑADE / VERIFICA ESTE
from extensions import db, csrf  
from sqlalchemy.exc import IntegrityError  # Añadir al inicio del archivo
from datetime import datetime, date, timedelta  # Agrega esto al inicio con las demás importaciones
from flask_wtf.csrf import  generate_csrf, validate_csrf, CSRFError
from flask_migrate import Migrate
from collections import Counter, defaultdict, OrderedDict
from calendar import month_name
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import joinedload, selectinload
from urllib.parse import urlencode
from werkzeug.datastructures import ImmutableMultiDict
from billing_utils import ( money, normalize_method, requires_reference, parse_conditions,
    compute_full_net, compute_full_net_from_pago,
)
from plan_utils import (get_or_create_plan, find_open_plan, registrar_abono, liquidar_plan
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'una-clave-secreta-muy-segura-aqui'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ballet.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicializa extensiones
db.init_app(app)
csrf.init_app(app)            # ✅ usa el csrf compartido de extensions
migrate = Migrate(app, db)    # ✅


# Ahora importa los modelos y forms
with app.app_context():
    from models import (
        Tutor, Estudiante, Instructor, Grupo, ContactoEmergencia, Articulo, Pago, Venta, 
        VentaLinea, venta_pago, venta_articulo, Paquete, PaqueteItem, Abono, PlanCobro, 
        Liquidacion
        )
    from forms import (
        EstudianteForm, TutorForm, InstructorForm, GrupoForm, ArticuloForm, VentaForm, 
        PagoForm, PaqueteForm,PaqueteItemForm, AbonoLineaForm
        )
    
    # ❌ No al importar
    ## db.create_all()

    # ✅ Solo al correr manualmente
    #if __name__ == '__main__':
    #    with app.app_context():
    #        db.create_all()
    #    app.run(debug=True)

@app.context_processor
def inject_csrf():
    return dict(csrf_token=generate_csrf)

@app.template_filter('from_json')
def from_json_filter(s):
    try:
        return json.loads(s)
    except Exception:
        return {}

@app.template_filter('fromjson')
def fromjson_filter(data):
    if not data:
        return {}
    return json.loads(data)

# Rutas
@app.context_processor
def utility_processor():
    def calcular_edad(fecha_nac):
        if not fecha_nac:
            return '-'
        hoy = date.today()
        return hoy.year - fecha_nac.year - ((hoy.month, hoy.day) < (fecha_nac.month, fecha_nac.day))
    
    return dict(
        calcular_edad=calcular_edad,
        any=any  # Añadimos la función any al contexto
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/registro')
def registro():
    return render_template('registro.html')

@app.route('/consulta')
def consulta():
    return render_template('consulta.html')

# ===== Helpers comunes =====
MESES_ES = ["", "Enero","Febrero","Marzo","Abril","Mayo","Junio",
            "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

def _cliente_nombre_y_tipo(est, ins):
    if est:
        apm = est.Est_ApellidoM or ''
        return (f"{est.Est_Nombre} {est.Est_ApellidoP} {apm}".strip(), "Estudiante")
    if ins:
        apm = ins.Instructor_ApellidoM or ''
        return (f"{ins.Instructor_Nombre} {ins.Instructor_ApellidoP} {apm}".strip(), "Instructor")
    return ("—", "N/D")

def _aplicar_filtros_ventas(q):
    #"""Aplica filtros a la query de ventas, leyendo request.args."""#
    inicio_str = request.args.get('inicio', '').strip()
    fin_str    = request.args.get('fin', '').strip()
    metodo     = request.args.get('metodo', '').strip().lower()  # efectivo, tarjeta, etc (opcional, filtra por pagos)
    tipo       = request.args.get('tipo', '').strip().lower()    # 'estudiante' | 'instructor' | '' (todos)
    qnombre    = request.args.get('q', '').strip()               # búsqueda por nombre del cliente

    inicio = None
    fin    = None
    if inicio_str:
        try:
            inicio = datetime.strptime(inicio_str, '%Y-%m-%d')
        except:
            flash('Fecha "inicio" inválida. Use YYYY-MM-DD.', 'warning')
    if fin_str:
        try:
            fin = datetime.strptime(fin_str, '%Y-%m-%d')
            fin = fin.replace(hour=23, minute=59, second=59, microsecond=999999)
        except:
            flash('Fecha "fin" inválida. Use YYYY-MM-DD.', 'warning')

    if inicio:
        q = q.filter(Venta.Fecha_Venta >= inicio)
    if fin:
        q = q.filter(Venta.Fecha_Venta <= fin)

    # Filtro por tipo de cliente
    if tipo == 'estudiante':
        q = q.filter(Venta.Est_ID.isnot(None))
    elif tipo == 'instructor':
        q = q.filter(Venta.Instructor_ID.isnot(None))

    # Filtro por nombre (coalesce estudiante/instructor)
    if qnombre:
        nombre_expr = func.trim(
            func.coalesce(
                (Estudiante.Est_Nombre + ' ' + Estudiante.Est_ApellidoP + ' ' + func.coalesce(Estudiante.Est_ApellidoM, '')),
                (Instructor.Instructor_Nombre + ' ' + Instructor.Instructor_ApellidoP + ' ' + func.coalesce(Instructor.Instructor_ApellidoM, ''))
            )
        )
        q = q.filter(nombre_expr.ilike(f"%{qnombre}%"))

    # Filtro por método de pago: necesitamos ventas que tengan al menos un pago con ese método
    if metodo:
        q = (q.join(venta_pago, Venta.Venta_ID == venta_pago.c.venta_id)
              .join(Pago, Pago.Pago_ID == venta_pago.c.pago_id)
              .filter(func.lower(Pago.Pago_Tipo) == metodo))

        # Ojo: hemos introducido joins adicionales; para evitar duplicados, usamos distinct en Venta
        q = q.distinct(Venta.Venta_ID)

        # Volvemos a unir estudiante/instructor para poder renderizar
        q = (q.outerjoin(Estudiante, Venta.Est_ID == Estudiante.Est_ID)
               .outerjoin(Instructor, Venta.Instructor_ID == Instructor.Instructor_ID))

    return q

def _sum_pagado_query(q_filtrada_base):
    #"""Devuelve la suma total de pagos para las ventas en la consulta filtrada."""#
    subq = q_filtrada_base.with_entities(Venta.Venta_ID).subquery()
    total = (db.session.query(func.coalesce(func.sum(Pago.Pago_Monto), 0.0))
             .select_from(Venta)
             .join(venta_pago, Venta.Venta_ID == venta_pago.c.venta_id)
             .join(Pago, Pago.Pago_ID == venta_pago.c.pago_id)
             .filter(Venta.Venta_ID.in_(db.session.query(subq.c.Venta_ID)))
             .scalar())
    return float(total or 0.0)

def _metodos_breakdown(q_filtrada_base):
    #"""Conteo/suma por método de pago sobre la consulta filtrada."""#
    subq = q_filtrada_base.with_entities(Venta.Venta_ID).subquery()
    rows = (db.session.query(Pago.Pago_Tipo, func.count(Pago.Pago_ID), func.coalesce(func.sum(Pago.Pago_Monto), 0.0))
            .select_from(Venta)
            .join(venta_pago, Venta.Venta_ID == venta_pago.c.venta_id)
            .join(Pago, Pago.Pago_ID == venta_pago.c.pago_id)
            .filter(Venta.Venta_ID.in_(db.session.query(subq.c.Venta_ID)))
            .group_by(Pago.Pago_Tipo)
            .order_by(func.sum(Pago.Pago_Monto).desc())
            .all())
    return [{'tipo': r[0], 'conteo': int(r[1]), 'monto': float(r[2])} for r in rows]

# === RUTAS CRUD VENTAS ===
# ===== Helpers =====
def _normalize_method(val: str) -> str:
    v = (val or "").strip().lower()
    map_alias = {
        "cash": "efectivo",
        "tarjeta de crédito": "tarjeta",
        "tarjeta de debito": "tarjeta",
        "tarjeta de débito": "tarjeta",
        "debito": "tarjeta",
        "débito": "tarjeta",
        "transferencia bancaria": "transferencia",
        "depósito": "deposito",
        "deposito bancario": "deposito",
    }
    return map_alias.get(v, v)

def _parse_conds(raw):
    import json, re
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(c).strip().lower() for c in data if str(c).strip()]
        if isinstance(data, str):
            raw = data
    except Exception:
        pass
    parts = re.split(r'[,;|]+', str(raw))
    return [p.strip().lower() for p in parts if p.strip()]

def _is_empty_meta(val) -> bool:
    if val is None:
        return True
    if isinstance(val, (list, tuple, set, dict)):
        return len(val) == 0
    s = str(val).strip()
    if s in ("", "[]", "{}", "null", "None", "NULL"):
        return True
    try:
        import json
        j = json.loads(s)
        if isinstance(j, (list, dict)) and len(j) == 0:
            return True
    except Exception:
        pass
    return False

def _is_autopago_unit(p, metodo_norm: str, subtotal_items: float) -> bool:
    #""Detecta el autopago auto-generado cuando no hay promos."""#
    try:
        tipo_norm = _normalize_method(getattr(p, 'Pago_Tipo', '') or '')
        monto     = float(getattr(p, 'Pago_Monto', 0.0) or 0.0)
        desc_tipo = getattr(p, 'Pago_Descuento_Tipo', None)
        desc_pct  = getattr(p, 'Pago_Descuento_Porcentaje', None)
        conds     = getattr(p, 'Pago_Condiciones', None)
        vence     = getattr(p, 'Pago_Restricciones_Fecha', None)
    except Exception:
        return False
    tol = max(0.05, 0.001 * max(1.0, float(subtotal_items)))
    mismo_monto = abs(monto - float(subtotal_items)) <= tol
    sin_desc    = _is_empty_meta(desc_tipo) and (desc_pct is None or float(desc_pct or 0) == 0.0)
    sin_restr   = _is_empty_meta(conds) and (vence is None)
    return (tipo_norm == metodo_norm) and mismo_monto and sin_desc and sin_restr

def _is_autopago_group(pagos, metodo_norm: str, subtotal_items: float) -> bool:
    #"""Todos lucen como autopago y la suma ≈ subtotal_items."""#
    if not pagos:
        return False
    try:
        total = sum(float(getattr(p, 'Pago_Monto', 0.0) or 0.0) for p in pagos)
    except Exception:
        return False
    tol = max(0.05, 0.001 * max(1.0, float(subtotal_items)))
    if abs(total - float(subtotal_items)) > tol:
        return False
    for p in pagos:
        tipo_norm = _normalize_method(getattr(p, 'Pago_Tipo', '') or '')
        desc_tipo = getattr(p, 'Pago_Descuento_Tipo', None)
        desc_pct  = getattr(p, 'Pago_Descuento_Porcentaje', None)
        conds     = getattr(p, 'Pago_Condiciones', None)
        vence     = getattr(p, 'Pago_Restricciones_Fecha', None)
        sin_desc  = _is_empty_meta(desc_tipo) and (desc_pct is None or float(desc_pct or 0) == 0.0)
        sin_restr = _is_empty_meta(conds) and (vence is None)
        if not ((tipo_norm == metodo_norm) and sin_desc and sin_restr):
            return False
    return True

def _armar_reporte(ventas_raw):
    #"""Devuelve (ventas, kpis) calculando:
    #   - subtotal de artículos
    #   - descuentos solo sobre pagos válidos por método/fecha
    #   - ignorando autopagos para no duplicar totales
    #"""
    ventas = []
    kpi_subtotal_items = 0.0
    kpi_descuentos     = 0.0
    kpi_total_general  = 0.0

    for v in ventas_raw:
        # Cliente
        if getattr(v, 'Est_ID', None):
            cli_tipo = 'estudiante'
            cli_nombre = f"{getattr(v.estudiante, 'Est_Nombre', '')} {getattr(v.estudiante, 'Est_ApellidoP', '')}".strip()
        elif getattr(v, 'Instructor_ID', None):
            cli_tipo = 'instructor'
            cli_nombre = f"{getattr(v.instructor, 'Instructor_Nombre', '')} {getattr(v.instructor, 'Instructor_ApellidoP', '')}".strip()
        else:
            cli_tipo = '—'
            cli_nombre = '—'

        # Líneas (sin descuento)
        items = []
        subtotal_items = 0.0
        for ln in (v.lineas or []):
            art_nombre = getattr(getattr(ln, 'articulo', None), 'Articulo_Nombre', None)
            if not art_nombre:
                try:
                    art = db.session.get(Articulo, ln.Articulo_ID)
                    art_nombre = art.Articulo_Nombre if art else f"ID {ln.Articulo_ID}"
                except Exception:
                    art_nombre = f"ID {ln.Articulo_ID}"
            cantidad = int(getattr(ln, 'Cantidad', 0) or 0)
            p_unit   = float(getattr(ln, 'Precio_Unitario', 0.0) or 0.0)
            total_ln = cantidad * p_unit
            subtotal_items += total_ln
            items.append({
                'articulo': art_nombre,
                'talla': getattr(ln, 'Talla', None),
                'cantidad': cantidad,
                'precio_unit': p_unit,
                'total_linea': total_ln
            })

        # Pagos/promos válidos (filtrando autopagos)
        metodo_norm = _normalize_method(getattr(v, 'Metodo_Pago', '') or '')
        venta_dt = getattr(v, 'Fecha_Venta', None)

        if cli_tipo == 'instructor':
            pagos_validos = []
        else:
            pagos_validos = []
            for p in (v.pagos or []):
                if _is_autopago_unit(p, metodo_norm, subtotal_items):
                    continue
                pagos_validos.append(p)
            if (not pagos_validos) and _is_autopago_group((v.pagos or []), metodo_norm, subtotal_items):
                pagos_validos = []

        pagos_view = []
        pagos_subtotal = 0.0
        desc_total_pagos = 0.0
        pagos_total_neto = 0.0

        for p in pagos_validos:
            monto_bruto = float(getattr(p, 'Pago_Monto', 0.0) or 0.0)
            pagos_subtotal += monto_bruto
            try:
                pct = float(p.Pago_Descuento_Porcentaje) if p.Pago_Descuento_Porcentaje is not None else 0.0
            except Exception:
                pct = 0.0
            conds_raw = getattr(p, 'Pago_Condiciones', None)
            conds_list = _parse_conds(conds_raw) if not _is_empty_meta(conds_raw) else []
            condiciones_ok = (len(conds_list) == 0) or (metodo_norm in [str(c).strip().lower() for c in conds_list])

            vence = getattr(p, 'Pago_Restricciones_Fecha', None)
            vigente = True
            if vence and venta_dt:
                try:
                    vence_d = vence.date() if hasattr(vence, 'date') else vence
                    venta_d = venta_dt.date() if hasattr(venta_dt, 'date') else venta_dt
                    vigente = (venta_d <= vence_d)
                except Exception:
                    vigente = True

            pct_aplicado = pct if (pct > 0 and condiciones_ok and vigente) else 0.0
            desc_monto = round(monto_bruto * (pct_aplicado/100.0), 2)
            monto_neto = round(monto_bruto - desc_monto, 2)

            desc_total_pagos += desc_monto
            pagos_total_neto += monto_neto

            pagos_view.append({
                'tipo': getattr(p, 'Pago_Tipo', ''),
                'monto_bruto': monto_bruto,
                'pct_aplicado': pct_aplicado,
                'descuento_monto': desc_monto,
                'monto_neto': monto_neto,
                'cond': getattr(p, 'Pago_Condiciones', None),
                'vence': getattr(p, 'Pago_Restricciones_Fecha', None),
            })

        total_venta = round(subtotal_items + pagos_total_neto, 2)

        ventas.append({
            'id': getattr(v, 'Venta_ID', None),
            'fecha': venta_dt,
            'cliente_tipo': cli_tipo,
            'cliente_nombre': cli_nombre or '—',
            'metodo': getattr(v, 'Metodo_Pago', '') or '—',
            'referencia': getattr(v, 'Referencia_Pago', None),
            'items': items,
            'subtotal_items': round(subtotal_items, 2),
            'pagos': pagos_view,
            'pagos_subtotal': round(pagos_subtotal, 2),
            'descuento_pagos': round(desc_total_pagos, 2),
            'total_venta': total_venta,
        })

        kpi_subtotal_items += subtotal_items
        kpi_descuentos     += desc_total_pagos
        kpi_total_general  += total_venta

    kpis = {
        'total_ventas': len(ventas),
        'sum_items': round(kpi_subtotal_items, 2),
        'sum_descuentos': round(kpi_descuentos, 2),
        'sum_total': round(kpi_total_general, 2)
    }
    return ventas, kpis

def _pendiente_expr():
    if hasattr(Venta, 'Cobro_Pendiente'):
        return or_(
            Venta.Cobro_Pendiente == True,
            func.lower(Venta.Metodo_Pago) == '__pendiente__',
            func.lower(Venta.Metodo_Pago) == 'pendiente',
            Venta.Metodo_Pago.is_(None),
            Venta.Metodo_Pago == ''
        )
    return or_(
        func.lower(Venta.Metodo_Pago) == '__pendiente__',
        func.lower(Venta.Metodo_Pago) == 'pendiente',
        Venta.Metodo_Pago.is_(None),
        Venta.Metodo_Pago == ''
    )


##----------------------------------------
## Tutor
##----------------------------------------

## Registro de tutor
##----------------------------------------
@app.route('/registro/tutor', methods=['GET', 'POST'])
def registro_tutor():
    form = TutorForm()
    
    if form.validate_on_submit():
        try:
            nuevo_tutor = Tutor(
                Tutor_Nombre=form.nombre.data,
                Tutor_ApellidoP=form.apellido_paterno.data,
                Tutor_ApellidoM=form.apellido_materno.data,
                Tutor_Celular=form.celular.data,
                Tutor_Edad=form.edad.data,
                Tutor_Parentesco=form.parentesco.data,
                Tutor_Correo=form.correo.data,
                Tutor_Ocupacion=form.ocupacion.data,
                Tutor_Facebook=form.facebook.data,
                Tutor_Instagram=form.instagram.data,
                Tutor_Direccion=form.direccion.data,
                Tutor_Medio_Entero=form.medio_entero.data
            )
            
            db.session.add(nuevo_tutor)
            db.session.commit()
            flash('Tutor registrado exitosamente!', 'success')
            return redirect(url_for('consulta_tutores'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error al registrar tutor: {str(e)}', 'danger')
    
    return render_template('registro_tutor.html', form=form)

## Consulta de tutor
##----------------------------------------
@app.route('/consulta/tutores')
def consulta_tutores():
    orden = request.args.get('orden', 'apellido')  # Valor por defecto
    
    if orden == 'nombre':
        tutores = Tutor.query.order_by(Tutor.Tutor_Nombre.asc()).all()
    else:  # Orden por apellido (por defecto)
        tutores = Tutor.query.order_by(
            Tutor.Tutor_ApellidoP.asc(),
            Tutor.Tutor_Nombre.asc()
        ).all()
    
    return render_template('consulta_tutores.html', 
                         tutores=tutores,
                         orden_actual=orden)

## Editar tutor
##----------------------------------------
@app.route('/editar/tutor/<int:id>', methods=['GET', 'POST'])
def editar_tutor(id):
    tutor = Tutor.query.get_or_404(id)
    
    if request.method == 'POST':
        tutor.Tutor_Nombre = request.form['nombre']
        tutor.Tutor_ApellidoP = request.form['apellido_paterno']
        tutor.Tutor_ApellidoM = request.form['apellido_materno']
        tutor.Tutor_Celular = request.form['celular']
        tutor.Tutor_Edad = request.form['edad']
        tutor.Tutor_Parentesco = request.form['parentesco']
        tutor.Tutor_Correo = request.form.get('correo')
        tutor.Tutor_Ocupacion = request.form.get('ocupacion')
        tutor.Tutor_Facebook = request.form.get('facebook')
        tutor.Tutor_Instagram = request.form.get('instagram')
        tutor.Tutor_Direccion = request.form.get('direccion')
        tutor.Tutor_Medio_Entero = request.form.get('medio_entero')
        
        db.session.commit()
        flash('Tutor actualizado correctamente', 'success')
        return redirect(url_for('consulta_tutores'))
    
    return render_template('editar_tutor.html', tutor=tutor)

## Eliminar tutor
##----------------------------------------
@app.route('/eliminar/tutor/<int:id>', methods=['POST'])
def eliminar_tutor(id):
    tutor = Tutor.query.get_or_404(id)
    
    try:
        # Verificar si el tutor tiene estudiantes asociados
        if tutor.estudiantes:
            flash('No se puede eliminar el tutor porque tiene estudiantes asociados', 'danger')
        else:
            db.session.delete(tutor)
            db.session.commit()
            flash('Tutor eliminado correctamente', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error al eliminar tutor: {str(e)}', 'danger')
    
    return redirect(url_for('consulta_tutores'))



##----------------------------------------
## Instructor
##----------------------------------------

## Registro de instructor
##----------------------------------------
@app.route('/registro/instructor', methods=['GET', 'POST'])
def registro_instructor():
    form = InstructorForm()
    
    if form.validate_on_submit():
        try:
            nuevo_instructor = Instructor(
                Instructor_Nombre=form.nombre.data,
                Instructor_ApellidoP=form.apellido_paterno.data,
                Instructor_ApellidoM=form.apellido_materno.data
            )
            db.session.add(nuevo_instructor)
            db.session.commit()
            flash('Instructor registrado exitosamente!', 'success')
            return redirect(url_for('consulta_instructores'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error al registrar instructor: {str(e)}', 'danger')
    
    return render_template('registro_instructor.html', form=form)

## Consulta de instructor
##----------------------------------------
@app.route('/consulta/instructores')
def consulta_instructores():
    instructores = Instructor.query.order_by(Instructor.Instructor_ApellidoP).all()
    return render_template('consulta_instructores.html', instructores=instructores)

## Editar instructor
##----------------------------------------
@app.route('/editar/instructor/<int:id>', methods=['GET', 'POST'])
def editar_instructor(id):
    instructor = Instructor.query.get_or_404(id)
    form = InstructorForm(obj=instructor)
    
    if request.method == 'GET':
        # Precargar manualmente los datos en el formulario
        form.nombre.data = instructor.Instructor_Nombre
        form.apellido_paterno.data = instructor.Instructor_ApellidoP
        form.apellido_materno.data = instructor.Instructor_ApellidoM

    if form.validate_on_submit():
        try:
            # Actualizar los campos manualmente para asegurarnos
            instructor.Instructor_Nombre = form.nombre.data
            instructor.Instructor_ApellidoP = form.apellido_paterno.data
            instructor.Instructor_ApellidoM = form.apellido_materno.data
            
            db.session.commit()
            flash('Instructor actualizado correctamente', 'success')
            return redirect(url_for('consulta_instructores'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error al actualizar instructor: {str(e)}', 'danger')
            app.logger.error(f"Error al actualizar instructor: {str(e)}")  # Para registro de errores

    return render_template('editar_instructor.html', form=form, instructor=instructor)

## Eliminar instrcutor
##----------------------------------------
@app.route('/eliminar/instructor/<int:id>', methods=['POST'])
def eliminar_instructor(id):
    instructor = Instructor.query.get_or_404(id)
    try:
        db.session.delete(instructor)
        db.session.commit()
        flash('Instructor eliminado', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error: {str(e)}', 'danger')
    return redirect(url_for('consulta_instructores'))


##----------------------------------------
## Grupo
##----------------------------------------

## Registro de grupo
##----------------------------------------
@app.route('/registro/grupo', methods=['GET', 'POST'])
def registro_grupo():
    form = GrupoForm()
    if form.validate_on_submit():
        try:
            nuevo_grupo = Grupo(
                Grupo_Nombre=form.nombre.data,
                Grupo_Horario=form.horario.data,
                Grupo_Dias=form.dias.data,
                Grupo_Nivel=form.nivel.data,
                Instructor_ID=form.instructor_id.data
            )
            db.session.add(nuevo_grupo)
            db.session.commit()
            flash('Grupo registrado exitosamente!', 'success')
            return redirect(url_for('consulta_grupos'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error al registrar grupo: {str(e)}', 'danger')
    
    return render_template('registro_grupo.html', form=form)

## Consulta de grupo
##----------------------------------------
# Consulta Grupo
@app.route('/consulta/grupos')
def consulta_grupos():
    grupos = Grupo.query.options(db.joinedload(Grupo.instructor)).all()
    return render_template('consulta_grupos.html', grupos=grupos)

## Editar grupo
##----------------------------------------
@app.route('/editar/grupo/<int:id>', methods=['GET', 'POST'])
def editar_grupo(id):
    grupo = Grupo.query.get_or_404(id)
    form = GrupoForm(obj=grupo)
    
    # Actualizar las opciones del instructor (importante para el select)
    form.instructor_id.choices = [(i.Instructor_ID, f"{i.Instructor_Nombre} {i.Instructor_ApellidoP}") 
                                for i in Instructor.query.order_by(Instructor.Instructor_Nombre).all()]

    if request.method == 'GET':
        # Precargar manualmente los datos en el formulario
        form.nombre.data = grupo.Grupo_Nombre
        form.horario.data = grupo.Grupo_Horario
        form.dias.data = grupo.Grupo_Dias
        form.nivel.data = grupo.Grupo_Nivel
        form.instructor_id.data = grupo.Instructor_ID

    if form.validate_on_submit():
        try:
            # Actualizar los campos manualmente para asegurarnos
            grupo.Grupo_Nombre = form.nombre.data
            grupo.Grupo_Horario = form.horario.data
            grupo.Grupo_Dias = form.dias.data
            grupo.Grupo_Nivel = form.nivel.data
            grupo.Instructor_ID = form.instructor_id.data
            
            db.session.commit()
            flash('Grupo actualizado exitosamente!', 'success')
            return redirect(url_for('consulta_grupos'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error al actualizar grupo: {str(e)}', 'danger')

    return render_template('editar_grupo.html', form=form, grupo=grupo)

## Eliminar grupo
##----------------------------------------
@app.route('/eliminar/grupo/<int:id>', methods=['POST'])
def eliminar_grupo(id):
    grupo = Grupo.query.get_or_404(id)
    try:
        db.session.delete(grupo)
        db.session.commit()
        flash('Grupo eliminado', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error: {str(e)}', 'danger')
    return redirect(url_for('consulta_grupos'))



##----------------------------------------
## Estudiante
##----------------------------------------

## Registro de estudiante
##----------------------------------------
'''@app.route('/registro/estudiante', methods=['GET', 'POST'])
def registro_estudiante():
    import json
    from datetime import datetime, date

    form = EstudianteForm()

    if form.validate_on_submit():
        try:
            # =========================
            # 1) Condiciones de salud
            # =========================
            condiciones = {}
            for campo in ['pie_plano', 'escoliosis', 'genu_varo', 'genu_valgo',
                          'desviacion_cadera', 'asma', 'psicopatologias']:
                if getattr(form, campo).data:
                    condiciones[campo] = True
            if form.otras_condiciones.data:
                condiciones['otras'] = form.otras_condiciones.data

            # ============================================
            # 2) Reingreso (marcar, fecha y nota opcional)
            # ============================================
            fecha_reingreso = None
            nota_reingreso = None
            if getattr(form, 'marcar_reingreso', None) and form.marcar_reingreso.data:
                # Si no capturan fecha, toma hoy
                fecha_reingreso = (form.fecha_reingreso.data or datetime.utcnow().date())
                # Nota opcional, limpia espacios
                nota_reingreso = (form.nota_reingreso.data or '').strip() or None

            # ========================================
            # 3) Crear Estudiante (incluye reingreso)
            # ========================================
            nuevo_estudiante = Estudiante(
                Est_Nombre=form.nombre.data,
                Est_ApellidoP=form.apellido_paterno.data,
                Est_ApellidoM=form.apellido_materno.data,
                Est_FechaNac=form.fecha_nacimiento.data,
                Est_Sexo=form.sexo.data,
                Tutor_ID=form.tutor_id.data,
                Est_LugarNac=form.lugar_nacimiento.data,
                Est_GradoEscolar=form.grado_escolar.data,
                Est_FechaIngreso=form.fecha_ingreso.data or datetime.utcnow(),
                Est_Colegio=form.colegio.data,
                Est_OtrasDisciplinas=form.otras_disciplinas.data,
                Est_MotivoIngreso=form.motivo_ingreso.data,
                Est_Status=form.status.data,
                Est_CondicionSalud=json.dumps(condiciones),
                Est_Alergias=form.alergias.data,
                Est_Medicamentos=form.medicamentos.data,
                # >>> Nuevos campos de reingreso <<<
                Est_FechaReingreso=fecha_reingreso,
                Est_Reingreso_Nota=nota_reingreso
            )

            db.session.add(nuevo_estudiante)
            db.session.flush()  # obtener Est_ID

            # ============================================
            # 4) Asignar grupos (checkboxes booleanos)
            #    - form.grupos es FieldList(BooleanField)
            #    - form.grupos_disponibles contiene los objetos Grupo en el mismo orden
            # ============================================
            grupos_elegidos = []
            if hasattr(form, 'grupos') and hasattr(form, 'grupos_disponibles'):
                for idx, entrada in enumerate(form.grupos.entries):
                    if entrada.data is True and idx < len(form.grupos_disponibles):
                        grupos_elegidos.append(form.grupos_disponibles[idx])
            if grupos_elegidos:
                nuevo_estudiante.grupos = grupos_elegidos

            # ============================================
            # 5) Contactos de emergencia
            # ============================================
            # Principal (obligatorio por tu validador)
            cp = form.contacto_principal.data
            contacto1 = ContactoEmergencia(
                Est_ID=nuevo_estudiante.Est_ID,
                Contacto_Nombre=cp['nombre'],
                Contacto_ApellidoP=cp['apellido_paterno'],
                Contacto_ApellidoM=cp.get('apellido_materno'),
                Contacto_Telefono=cp['telefono'],
                Contacto_Parentesco=cp['parentesco']
            )
            db.session.add(contacto1)

            # Secundario (opcional)
            cs = form.contacto_secundario.data
            if cs.get('nombre'):
                contacto2 = ContactoEmergencia(
                    Est_ID=nuevo_estudiante.Est_ID,
                    Contacto_Nombre=cs['nombre'],
                    Contacto_ApellidoP=cs['apellido_paterno'],
                    Contacto_ApellidoM=cs.get('apellido_materno'),
                    Contacto_Telefono=cs['telefono'],
                    Contacto_Parentesco=cs['parentesco']
                )
                db.session.add(contacto2)

            db.session.commit()
            flash('Estudiante registrado exitosamente!', 'success')
            return redirect(url_for('consulta_estudiantes'))

        except Exception as e:
            db.session.rollback()
            app.logger.exception("Error en registro estudiante")
            flash(f'Error al registrar estudiante: {str(e)}', 'danger')

    elif request.method == 'POST':
        # Hubo POST pero no pasó validación WTForms
        flash('Por favor corrige los errores en el formulario.', 'warning')

    return render_template('registro_estudiante.html', form=form)
'''

## Consulta de estudiante
##----------------------------------------
@app.route('/consulta/estudiantes')
def consulta_estudiantes():
    """
    Parámetros GET soportados:
      - orden: apellido (def), nombre, fecha, ingreso, reingreso
      - status: Activo | Inactivo | Egresado
      - busqueda: texto libre (nombre, apellidos, colegio)
      - con_reingreso: 'si' | 'no'  (opcional)
    """
    orden = request.args.get('orden', 'apellido')
    status = request.args.get('status', '').strip()
    busqueda = (request.args.get('busqueda', '') or '').strip()
    con_reingreso = (request.args.get('con_reingreso', '') or '').strip().lower()

    query = (Estudiante.query
             .options(
                 joinedload(Estudiante.grupos),
                 joinedload(Estudiante.contactos_emergencia),
                 joinedload(Estudiante.tutor))
            )

    # Filtro por status
    if status:
        query = query.filter(Estudiante.Est_Status == status)

    # Filtro por reingreso
    if con_reingreso == 'si':
        query = query.filter(Estudiante.Est_FechaReingreso.isnot(None))
    elif con_reingreso == 'no':
        query = query.filter(Estudiante.Est_FechaReingreso.is_(None))

    # Búsqueda libre
    if busqueda:
        b = f"%{busqueda}%"
        query = query.filter(
            or_(
                Estudiante.Est_Nombre.ilike(b),
                Estudiante.Est_ApellidoP.ilike(b),
                Estudiante.Est_ApellidoM.ilike(b),
                Estudiante.Est_Colegio.ilike(b)
            )
        )

    # Ordenamiento
    if orden == 'nombre':
        query = query.order_by(Estudiante.Est_Nombre.asc(),
                               Estudiante.Est_ApellidoP.asc())
    elif orden == 'fecha':
        query = query.order_by(Estudiante.Est_FechaNac.asc(),
                               Estudiante.Est_ApellidoP.asc())
    elif orden == 'ingreso':
        query = query.order_by(Estudiante.Est_FechaIngreso.asc(),
                               Estudiante.Est_ApellidoP.asc())
    elif orden == 'reingreso':
        # Primero los que SÍ tienen reingreso (NULLS LAST/ FIRST depende del motor)
        # Aquí: ordena por Est_FechaReingreso asc, dejando None al final.
        query = query.order_by(Estudiante.Est_FechaReingreso.is_(None),
                               Estudiante.Est_FechaReingreso.asc(),
                               Estudiante.Est_ApellidoP.asc())
    else:  # apellido (default)
        query = query.order_by(Estudiante.Est_ApellidoP.asc(),
                               Estudiante.Est_Nombre.asc())

    estudiantes = query.all()

    return render_template(
        'consulta_estudiantes.html',
        estudiantes=estudiantes,
        orden_actual=orden,
        status_actual=status,
        busqueda=busqueda,
        con_reingreso=con_reingreso
    )


## Editar estudiante
##----------------------------------------
@app.route('/editar/estudiante/<int:id>', methods=['GET', 'POST'])
def editar_estudiante(id):
    estudiante = (Estudiante.query
                  .options(joinedload(Estudiante.contactos_emergencia),
                           joinedload(Estudiante.grupos))
                  .get_or_404(id))

    tutores = Tutor.query.order_by(Tutor.Tutor_ApellidoP, Tutor.Tutor_Nombre).all()
    grupos_disponibles = (Grupo.query
                          .options(db.joinedload(Grupo.instructor))
                          .order_by(Grupo.Grupo_Nombre.asc())
                          .all())

    if request.method == 'POST':
        try:
            # ========= Campos básicos =========
            estudiante.Est_Nombre = request.form['nombre'].strip()
            estudiante.Est_ApellidoP = request.form['apellido_paterno'].strip()
            estudiante.Est_ApellidoM = request.form.get('apellido_materno', '').strip() or None

            estudiante.Est_FechaNac = datetime.strptime(
                request.form['fecha_nacimiento'], '%Y-%m-%d'
            ).date()

            estudiante.Est_Sexo = request.form['sexo']
            estudiante.Tutor_ID = int(request.form['tutor_id'])

            estudiante.Est_LugarNac = request.form.get('lugar_nacimiento', '').strip() or None
            estudiante.Est_GradoEscolar = request.form.get('grado_escolar', '').strip() or None
            estudiante.Est_Colegio = request.form.get('colegio', '').strip() or None
            estudiante.Est_OtrasDisciplinas = request.form.get('otras_disciplinas', '').strip() or None
            estudiante.Est_MotivoIngreso = request.form.get('motivo_ingreso', '').strip() or None
            estudiante.Est_Status = request.form['status']

            # Ingreso (opcional)
            if request.form.get('fecha_ingreso'):
                estudiante.Est_FechaIngreso = datetime.strptime(
                    request.form['fecha_ingreso'], '%Y-%m-%d'
                ).date()

            # ========= Reingreso =========
            # Convención del form (recomendado en tu template):
            # - checkbox name="marcar_reingreso"
            # - input date name="fecha_reingreso"
            # - input text name="nota_reingreso"
            # - input hidden name="limpiar_reingreso" value="1" si quieren limpiar
            marcar_reingreso = ('marcar_reingreso' in request.form)
            limpiar_reingreso = request.form.get('limpiar_reingreso', '0') == '1'
            fecha_reingreso_raw = request.form.get('fecha_reingreso', '').strip()
            nota_reingreso_raw = request.form.get('nota_reingreso', '').strip()

            if limpiar_reingreso:
                estudiante.Est_FechaReingreso = None
                estudiante.Est_Reingreso_Nota = None
            else:
                if marcar_reingreso:
                    # Si marcan reingreso, exigimos una fecha válida (o hoy, si quieres)
                    if fecha_reingreso_raw:
                        estudiante.Est_FechaReingreso = datetime.strptime(
                            fecha_reingreso_raw, '%Y-%m-%d'
                        ).date()
                    else:
                        # Por política de negocio: si no envían fecha, podrías usar hoy.
                        estudiante.Est_FechaReingreso = datetime.utcnow().date()
                    estudiante.Est_Reingreso_Nota = nota_reingreso_raw or None
                else:
                    # Si no se marca, conservar lo que haya (no tocar)
                    pass

            # ========= Salud =========
            estudiante.Est_Alergias = request.form.get('alergias', '').strip() or None
            estudiante.Est_Medicamentos = request.form.get('medicamentos', '').strip() or None

            condiciones = {
                'pie_plano': 'pie_plano' in request.form,
                'escoliosis': 'escoliosis' in request.form,
                'genu_varo': 'genu_varo' in request.form,
                'genu_valgo': 'genu_valgo' in request.form,
                'desviacion_cadera': 'desviacion_cadera' in request.form,
                'asma': 'asma' in request.form,
                'psicopatologias': 'psicopatologias' in request.form,
                'otras': request.form.get('otras_condiciones', '').strip()
            }
            estudiante.Est_CondicionSalud = json.dumps(condiciones)

            # ========= Grupos =========
            # En tu template de edición, renderiza checkboxes con name="grupos" y value="{{ Grupo_ID }}"
            grupos_seleccionados = set(request.form.getlist('grupos'))  # strings
            grupos_actuales = {str(g.Grupo_ID) for g in estudiante.grupos}

            # Agregar nuevos
            to_add = grupos_seleccionados - grupos_actuales
            if to_add:
                nuevos = Grupo.query.filter(Grupo.Grupo_ID.in_(list(map(int, to_add)))).all()
                for g in nuevos:
                    estudiante.grupos.append(g)

            # Quitar no seleccionados
            for g in estudiante.grupos[:]:
                if str(g.Grupo_ID) not in grupos_seleccionados:
                    estudiante.grupos.remove(g)

            # ========= Contactos emergencia =========
            # Mantiene tu convención:
            #   contacto_id_{i} (existentes)
            #   contacto_nombre_{i}, contacto_apellido_p_{i}, contacto_apellido_m_{i},
            #   contacto_telefono_{i}, contacto_parentesco_{i}
            #
            #   contacto_id_new_{j} para nuevos
            #   contacto_nombre_new_{j}, ...
            contactos_procesados = set()

            # Actualizar existentes
            for key in request.form.keys():
                if key.startswith('contacto_id_') and not key.endswith('_new'):
                    contacto_id = request.form[key]
                    contacto = ContactoEmergencia.query.get(contacto_id)
                    if contacto and contacto.Est_ID == estudiante.Est_ID:
                        idx = key.split('_')[-1]
                        contacto.Contacto_Nombre = request.form.get(f'contacto_nombre_{idx}', '').strip()
                        contacto.Contacto_ApellidoP = request.form.get(f'contacto_apellido_p_{idx}', '').strip()
                        contacto.Contacto_ApellidoM = request.form.get(f'contacto_apellido_m_{idx}', '').strip() or None
                        contacto.Contacto_Telefono = request.form.get(f'contacto_telefono_{idx}', '').strip()
                        contacto.Contacto_Parentesco = request.form.get(f'contacto_parentesco_{idx}', '').strip()
                        contactos_procesados.add(str(contacto.Contacto_ID))

            # Eliminar los que no vinieron
            for contacto in estudiante.contactos_emergencia[:]:
                if str(contacto.Contacto_ID) not in contactos_procesados:
                    db.session.delete(contacto)

            # Agregar nuevos
            for key in request.form.keys():
                if key.startswith('contacto_id_new_'):
                    idx = key.split('_')[-1]
                    nombre_n = request.form.get(f'contacto_nombre_new_{idx}', '').strip()
                    if not nombre_n:
                        continue  # ignora filas vacías
                    nuevo_contacto = ContactoEmergencia(
                        Est_ID=estudiante.Est_ID,
                        Contacto_Nombre=nombre_n,
                        Contacto_ApellidoP=request.form.get(f'contacto_apellido_p_new_{idx}', '').strip(),
                        Contacto_ApellidoM=request.form.get(f'contacto_apellido_m_new_{idx}', '').strip() or None,
                        Contacto_Telefono=request.form.get(f'contacto_telefono_new_{idx}', '').strip(),
                        Contacto_Parentesco=request.form.get(f'contacto_parentesco_new_{idx}', '').strip()
                    )
                    db.session.add(nuevo_contacto)

            db.session.commit()
            flash('Estudiante actualizado correctamente', 'success')
            return redirect(url_for('consulta_estudiantes'))

        except Exception as e:
            db.session.rollback()
            app.logger.exception("Error al actualizar estudiante")
            flash(f'Error al actualizar estudiante: {str(e)}', 'danger')

    # GET
    return render_template('editar_estudiante.html',
                           estudiante=estudiante,
                           tutores=tutores,
                           grupos_disponibles=grupos_disponibles)


## Eliminar estudiante
##----------------------------------------
@csrf.exempt
@app.route('/eliminar/estudiante/<int:id>', methods=['POST'])
def eliminar_estudiante(id):
    """
    Elimina un estudiante validando CSRF manualmente para soportar tanto:
    - Formularios HTML (input hidden name='csrf_token')
    - Llamadas fetch/AJAX (header X-CSRFToken o X-CSRF-Token)
    """
    # 1) Validación CSRF manual (soporta form y headers)
    token = (
        request.form.get('csrf_token')
        or request.headers.get('X-CSRFToken')
        or request.headers.get('X-CSRF-Token')
    )
    try:
        validate_csrf(token)
    except CSRFError:
        flash('Operación no autorizada: falta o es inválido el CSRF token.', 'danger')
        return redirect(url_for('consulta_estudiantes'))

    # 2) Buscar estudiante
    estudiante = Estudiante.query.get_or_404(id)

    # 3) Intentar eliminar
    try:
        db.session.delete(estudiante)
        db.session.commit()
        flash('Estudiante eliminado correctamente', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error al eliminar el estudiante: {str(e)}', 'danger')

    # 4) Volver al listado
    return redirect(url_for('consulta_estudiantes'))


# --------------------------------------------------
# Registro Estudiante - Tutor
# --------------------------------------------------
from uuid import uuid4
from datetime import datetime, date, timedelta
from sqlalchemy import func

@app.route('/registro/tutor-estudiante', methods=['GET', 'POST'])
def registro_tutor_estudiante():
    import json

    # Instancias WTForms con prefijos (para GET inicial)
    tutor_form = TutorForm(prefix='tutor')
    est_form   = EstudianteForm(prefix='est')

    # -------- Prefill por ?reuse_tutor=ID + generar NONCE en GET --------
    reuse_tutor_id = request.args.get('reuse_tutor', type=int)

    if request.method == 'GET':
        # Nonce idempotente para este render
        session['reg_te_nonce'] = str(uuid4())

        if reuse_tutor_id:
            t = Tutor.query.get(reuse_tutor_id)
            if t:
                # Prefill visual del tutor
                tutor_form.nombre.data            = t.Tutor_Nombre
                tutor_form.apellido_paterno.data  = t.Tutor_ApellidoP
                tutor_form.apellido_materno.data  = t.Tutor_ApellidoM
                tutor_form.celular.data           = t.Tutor_Celular
                tutor_form.edad.data              = t.Tutor_Edad
                tutor_form.parentesco.data        = t.Tutor_Parentesco
                tutor_form.correo.data            = t.Tutor_Correo
                tutor_form.ocupacion.data         = t.Tutor_Ocupacion
                tutor_form.facebook.data          = t.Tutor_Facebook
                tutor_form.instagram.data         = t.Tutor_Instagram
                tutor_form.direccion.data         = t.Tutor_Direccion
                tutor_form.medio_entero.data      = t.Tutor_Medio_Entero

        return render_template(
            'registro_tutor_estudiante.html',
            tutor_form=tutor_form,
            form=est_form,
            reuse_tutor_id=reuse_tutor_id,
            reg_te_nonce=session.get('reg_te_nonce')
        )

    # ===================== POST =====================
    # Idempotencia: valida NONCE
    posted_nonce = request.form.get('reg_te_nonce')
    last_used = session.get('reg_te_last_used_nonce')

    if not posted_nonce:
        flash('Solicitud inválida. Refresca la página e inténtalo de nuevo.', 'warning')
        return redirect(url_for('registro_tutor_estudiante'))

    if posted_nonce == last_used:
        # Ya se procesó este formulario
        flash('Este formulario ya fue procesado.', 'info')
        return redirect(url_for('consulta_estudiantes'))

    # Reconstituye los forms desde request.form (mantén los prefijos)
    tutor_form = TutorForm(request.form, prefix='tutor')
    est_form   = EstudianteForm(request.form, prefix='est')

    action = request.form.get('action')  # 'save' | 'save_and_new_student'
    posted_reuse_id = request.form.get('reuse_tutor_id', type=int)

    # ---- Normaliza selects (WTForms 3 no tolera choices=None) ----
    def _ensure_select_choices(form):
        try:
            for f in form:
                if hasattr(f, 'choices') and f.choices is None:
                    f.choices = []
        except Exception:
            pass

    _ensure_select_choices(tutor_form)
    _ensure_select_choices(est_form)

    # ---- Neutraliza est.tutor_id para esta vista (NO usamos ese field aquí) ----
    if hasattr(est_form, 'tutor_id'):
        try:
            est_form.tutor_id.validators = []           # quita DataRequired
            est_form.tutor_id.flags.required = False
            est_form.tutor_id.validate_choice = False   # evita "Not a valid choice"
            if getattr(est_form.tutor_id, 'choices', None) is None:
                est_form.tutor_id.choices = []
            # si vienes reutilizando tutor, setea el value (no se usa, pero evita rarezas)
            if posted_reuse_id:
                est_form.tutor_id.data = posted_reuse_id
            else:
                if est_form.tutor_id.data in (None, '', 'None'):
                    est_form.tutor_id.data = None
        except Exception:
            pass

    # Validación: si reutilizas tutor, no validamos sus campos
    ok_tutor = tutor_form.validate() if not posted_reuse_id else True
    ok_est   = est_form.validate()

    if not (ok_tutor and ok_est):
        # Loggea errores; ignora tutor si se reutiliza
        if not posted_reuse_id:
            for field, errs in (tutor_form.errors or {}).items():
                try:
                    label_txt = getattr(tutor_form, field).label.text
                except Exception:
                    label_txt = field
                for e in errs:
                    app.logger.warning(f"Tutor - {label_txt}: {e}")

        for field, errs in (est_form.errors or {}).items():
            if field == 'tutor_id':
                continue
            try:
                label_txt = getattr(est_form, field).label.text
            except Exception:
                label_txt = field
            for e in errs:
                app.logger.warning(f"Estudiante - {label_txt}: {e}")

        flash('Por favor corrige los errores indicados.', 'warning')
        # Genera un nuevo nonce para el re-render (evita que el usuario se quede sin nonce)
        session['reg_te_nonce'] = str(uuid4())
        return render_template(
            'registro_tutor_estudiante.html',
            tutor_form=tutor_form,
            form=est_form,
            reuse_tutor_id=reuse_tutor_id,
            reg_te_nonce=session.get('reg_te_nonce')
        )

    # ===== Guardado =====
    try:
        # ---------- A) Tutor: crear o reutilizar + anti-duplicado ----------
        if posted_reuse_id:
            nuevo_tutor = Tutor.query.get(posted_reuse_id)
            if not nuevo_tutor:
                flash('No se encontró el tutor a reutilizar.', 'danger')
                return redirect(url_for('registro_tutor_estudiante'))
        else:
            # Anti-duplicado de Tutor por celular/correo si existen
            candidato = None
            cel = (tutor_form.celular.data or '').strip()
            cor = (tutor_form.correo.data or '').strip()

            if cel:
                candidato = Tutor.query.filter(func.trim(Tutor.Tutor_Celular) == cel).first()
            if not candidato and cor:
                candidato = Tutor.query.filter(func.lower(func.trim(Tutor.Tutor_Correo)) == cor.lower()).first()

            if candidato:
                nuevo_tutor = candidato
            else:
                nuevo_tutor = Tutor(
                    Tutor_Nombre=tutor_form.nombre.data,
                    Tutor_ApellidoP=tutor_form.apellido_paterno.data,
                    Tutor_ApellidoM=tutor_form.apellido_materno.data,
                    Tutor_Celular=tutor_form.celular.data,
                    Tutor_Edad=tutor_form.edad.data,
                    Tutor_Parentesco=tutor_form.parentesco.data,
                    Tutor_Correo=tutor_form.correo.data,
                    Tutor_Ocupacion=tutor_form.ocupacion.data,
                    Tutor_Facebook=tutor_form.facebook.data,
                    Tutor_Instagram=tutor_form.instagram.data,
                    Tutor_Direccion=tutor_form.direccion.data,
                    Tutor_Medio_Entero=tutor_form.medio_entero.data
                )
                db.session.add(nuevo_tutor)
                db.session.flush()  # obtiene Tutor_ID

        # ---------- B) Condiciones de salud ----------
        condiciones = {}
        for campo in ['pie_plano', 'escoliosis', 'genu_varo', 'genu_valgo',
                      'desviacion_cadera', 'asma', 'psicopatologias']:
            try:
                if getattr(est_form, campo).data:
                    condiciones[campo] = True
            except Exception:
                pass
        if getattr(est_form, 'otras_condiciones', None) and est_form.otras_condiciones.data:
            condiciones['otras'] = est_form.otras_condiciones.data

        # ---------- C) Reingreso ----------
        fecha_reingreso = None
        nota_reingreso  = None
        try:
            if getattr(est_form, 'marcar_reingreso', None) and est_form.marcar_reingreso.data:
                fecha_reingreso = est_form.fecha_reingreso.data or date.today()
                nota_reingreso  = (est_form.nota_reingreso.data or '').strip() or None
        except Exception:
            pass

        # ---------- D) Anti-duplicado Estudiante en ventana corta ----------
        ventana = datetime.utcnow() - timedelta(minutes=5)
        ya_existe = (Estudiante.query
            .filter(
                func.lower(func.trim(Estudiante.Est_Nombre))    == (est_form.nombre.data or '').strip().lower(),
                func.lower(func.trim(Estudiante.Est_ApellidoP)) == (est_form.apellido_paterno.data or '').strip().lower(),
                func.lower(func.coalesce(func.trim(Estudiante.Est_ApellidoM), '')) ==
                    (est_form.apellido_materno.data or '').strip().lower(),
                Estudiante.Est_FechaNac == est_form.fecha_nacimiento.data,
                Estudiante.Tutor_ID == nuevo_tutor.Tutor_ID,
                Estudiante.Est_FechaIngreso >= ventana
            ).first())

        if ya_existe:
            nuevo_estudiante = ya_existe
        else:
            nuevo_estudiante = Estudiante(
                Est_Nombre=est_form.nombre.data,
                Est_ApellidoP=est_form.apellido_paterno.data,
                Est_ApellidoM=est_form.apellido_materno.data,
                Est_FechaNac=est_form.fecha_nacimiento.data,
                Est_Sexo=est_form.sexo.data,
                Tutor_ID=nuevo_tutor.Tutor_ID,
                Est_LugarNac=est_form.lugar_nacimiento.data,
                Est_GradoEscolar=est_form.grado_escolar.data,
                Est_FechaIngreso=est_form.fecha_ingreso.data or datetime.utcnow(),
                Est_Colegio=est_form.colegio.data,
                Est_OtrasDisciplinas=est_form.otras_disciplinas.data,
                Est_MotivoIngreso=est_form.motivo_ingreso.data,
                Est_Status=est_form.status.data,
                Est_CondicionSalud=json.dumps(condiciones) if condiciones else None,
                Est_Alergias=getattr(est_form, 'alergias', None).data if hasattr(est_form, 'alergias') else None,
                Est_Medicamentos=getattr(est_form, 'medicamentos', None).data if hasattr(est_form, 'medicamentos') else None,
                Est_FechaReingreso=fecha_reingreso,
                Est_Reingreso_Nota=nota_reingreso
            )
            db.session.add(nuevo_estudiante)
            db.session.flush()  # Est_ID

        # ---------- E) Grupos ----------
        try:
            if hasattr(est_form, 'grupos') and hasattr(est_form, 'grupos_disponibles'):
                seleccionados = []
                for idx, entrada in enumerate(est_form.grupos.entries):
                    if entrada.data is True and idx < len(est_form.grupos_disponibles):
                        seleccionados.append(est_form.grupos_disponibles[idx])
                nuevo_estudiante.grupos = seleccionados
        except Exception as e:
            app.logger.warning(f"No se pudieron asignar grupos: {e}")

        # ---------- F) Contactos ----------
        cp_data = est_form.contacto_principal.data if hasattr(est_form, 'contacto_principal') else {}
        cp_nombre = (cp_data.get('nombre') or '').strip()
        cp_ap     = (cp_data.get('apellido_paterno') or '').strip()
        cp_tel    = (cp_data.get('telefono') or '').strip()
        if not (cp_nombre and cp_ap and cp_tel):
            raise ValueError("El contacto principal requiere nombre, apellido paterno y teléfono.")

        db.session.add(ContactoEmergencia(
            Est_ID=nuevo_estudiante.Est_ID,
            Contacto_Nombre=cp_nombre,
            Contacto_ApellidoP=cp_ap,
            Contacto_ApellidoM=(cp_data.get('apellido_materno') or '').strip(),
            Contacto_Telefono=cp_tel,
            Contacto_Parentesco=(cp_data.get('parentesco') or '').strip()
        ))

        usar_sec = (request.form.get('est-usar_contacto_secundario') == '1')
        if usar_sec and hasattr(est_form, 'contacto_secundario'):
            cs = est_form.contacto_secundario.data
            if cs and (cs.get('nombre') or '').strip():
                db.session.add(ContactoEmergencia(
                    Est_ID=nuevo_estudiante.Est_ID,
                    Contacto_Nombre=(cs.get('nombre') or '').strip(),
                    Contacto_ApellidoP=(cs.get('apellido_paterno') or '').strip(),
                    Contacto_ApellidoM=(cs.get('apellido_materno') or '').strip(),
                    Contacto_Telefono=(cs.get('telefono') or '').strip(),
                    Contacto_Parentesco=(cs.get('parentesco') or '').strip()
                ))

        # Commit y marca nonce como usado
        db.session.commit()
        session['reg_te_last_used_nonce'] = posted_nonce
        session.pop('reg_te_nonce', None)

        # Redirecciones
        if action == 'save_and_new_student':
            flash('Estudiante registrado. Puedes capturar otro para el mismo tutor.', 'success')
            reuse_id = posted_reuse_id or nuevo_tutor.Tutor_ID
            return redirect(url_for('registro_tutor_estudiante', reuse_tutor=reuse_id))
        else:
            flash('Tutor y Estudiante registrados exitosamente.', 'success')
            return redirect(url_for('consulta_estudiantes'))

    except Exception as e:
        db.session.rollback()
        app.logger.exception("Error en registro tutor-estudiante")
        flash(f'Error al registrar: {str(e)}', 'danger')
        # Genera nuevo nonce para reintento
        session['reg_te_nonce'] = str(uuid4())
        return render_template(
            'registro_tutor_estudiante.html',
            tutor_form=tutor_form,
            form=est_form,
            reuse_tutor_id=reuse_tutor_id,
            reg_te_nonce=session.get('reg_te_nonce')
        )




# --------------------------------------------------
# Artículos
# --------------------------------------------------

## Agregar articulos
##----------------------------------------
@app.route('/registro/articulo', methods=['GET', 'POST'])
def registro_articulo():
    form = ArticuloForm()
    
    if form.validate_on_submit():
        try:
            # Procesar las tallas si aplica
            tallas = {}
            if form.tipo_talla.data != 'ninguno':
                for item in form.tallas_numeros.data:
                    tallas[item['nombre']] = item['cantidad']

            nuevo_articulo = Articulo(
                Articulo_Nombre=form.nombre.data,
                Articulo_PrecioVenta=form.precio.data,
                Articulo_Existencia=form.existencia.data if form.tipo_talla.data == 'ninguno' else sum(tallas.values()),
                Articulo_TipoTalla=form.tipo_talla.data if form.tipo_talla.data != 'ninguno' else None,
                Articulo_Tallas=json.dumps(tallas) if tallas else None
            )

            db.session.add(nuevo_articulo)
            db.session.commit()
            flash('Artículo registrado exitosamente!', 'success')
            return redirect(url_for('registro_articulo'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error al registrar artículo: {str(e)}', 'danger')
            app.logger.error(f"Error en registro_articulo: {str(e)}")
    
    elif request.method == 'POST':
        flash('Por favor corrige los errores en el formulario.', 'warning')

    return render_template('registro_articulo.html', form=form)


## ---------------------------------------
## Consultar articulos
##----------------------------------------
@app.route('/consulta_articulos')
def consulta_articulos():
    import json
    from collections import Counter
    from sqlalchemy.orm import joinedload

    orden = request.args.get('orden', 'nombre')
    orden_paq = request.args.get('orden_paq', 'nombre')
    busqueda = request.args.get('busqueda', '')
    vista = request.args.get('vista', 'articulos')  # 'articulos' | 'paquetes'

    articulos = []
    paquetes = []

    # =========================
    #  ARTÍCULOS
    # =========================
    try:
        query = Articulo.query
        if busqueda:
            query = query.filter(Articulo.Articulo_Nombre.ilike(f'%{busqueda}%'))

        lista_articulos = query.all()

        # ➊ Contar duplicados (por nombre, case/espacios normalizados)
        nombres_norm = [((a.Articulo_Nombre or '').strip().lower()) for a in lista_articulos]
        name_counts = Counter(nombres_norm)

        for articulo in lista_articulos:
            precio = float(articulo.Articulo_PrecioVenta or 0)
            nombre = articulo.Articulo_Nombre or ''
            nombre_key = nombre.strip().lower()
            es_duplicado = name_counts.get(nombre_key, 0) > 1

            tipo_raw = (articulo.Articulo_TipoTalla or 'ninguno')
            tipo = str(tipo_raw).strip().lower()

            # ➋ SIN talla
            if not tipo or tipo == 'ninguno':
                articulos.append({
                    'id': articulo.Articulo_ID,
                    'nombre': nombre,
                    'talla_numero': '-',
                    'precio': precio,
                    'existencia': articulo.Articulo_Existencia,
                    'tipo': 'sin_talla',
                    'tipo_talla': tipo,              # extra para tooltip/debug
                    'raw_tallas': articulo.Articulo_Tallas,
                    'duplicado': es_duplicado,
                    'inconsistente': False           # consistente (no usa tallas)
                })
                continue

            # ➌ CON talla/número: intentar leer variantes
            variantes = []
            tallas_val = articulo.Articulo_Tallas
            try:
                tallas = json.loads(tallas_val) if tallas_val else {}
            except json.JSONDecodeError:
                tallas = {}

            if isinstance(tallas, dict):
                iterable = list(tallas.items())
            elif isinstance(tallas, list):
                iterable = [(str(x), None) for x in tallas]
            else:
                iterable = []

            if iterable:
                # Hay variantes reales ⇒ desplegar cada una
                for talla, existencia in iterable:
                    articulos.append({
                        'id': articulo.Articulo_ID,
                        'nombre': nombre,
                        'talla_numero': talla,
                        'precio': precio,
                        'existencia': existencia if existencia is not None else articulo.Articulo_Existencia,
                        'tipo': 'con_talla',
                        'tipo_talla': tipo,
                        'raw_tallas': articulo.Articulo_Tallas,
                        'duplicado': es_duplicado,
                        'inconsistente': False
                    })
            else:
                # ⚠ Tipo declarado pero SIN variantes ⇒ mostrar fila “fantasma” marcada como inconsistente
                articulos.append({
                    'id': articulo.Articulo_ID,
                    'nombre': nombre,
                    'talla_numero': '-',              # se ve en la columna Talla/Número
                    'precio': precio,
                    'existencia': articulo.Articulo_Existencia,
                    'tipo': 'con_talla',
                    'tipo_talla': tipo,
                    'raw_tallas': articulo.Articulo_Tallas,
                    'duplicado': es_duplicado,
                    'inconsistente': True
                })

        # Ordenamiento de artículos
        if orden == 'precio':
            articulos.sort(key=lambda x: (x['precio'], x['nombre'], str(x['talla_numero'])))
        elif orden == 'talla_numero':
            # Mantener '-' al final
            articulos.sort(key=lambda x: (x['talla_numero'] == '-', str(x['talla_numero']), x['nombre'], x['precio']))
        else:
            articulos.sort(key=lambda x: (x['nombre'].lower(), str(x['talla_numero']), x['precio']))

    except Exception as e:
        app.logger.error(f"Error en consulta_articulos (artículos): {str(e)}")
        flash('Ocurrió un error al obtener los artículos', 'danger')

    # =========================
    #  PAQUETES (leer solo talla_numero)
    # =========================
    try:
        paquetes_q = (Paquete.query
                      .options(joinedload(Paquete.items).joinedload(PaqueteItem.articulo)))
        if busqueda:
            paquetes_q = paquetes_q.filter(Paquete.nombre.ilike(f'%{busqueda}%'))

        paquetes_db = paquetes_q.all()

        for p in paquetes_db:
            items_rel = getattr(p, 'items', None)
            if items_rel is None:
                items_rel = PaqueteItem.query.filter_by(paquete_id=p.id).all()

            items = []
            total_lista = 0.0
            total_cantidades = 0

            for it in items_rel:
                art_obj = getattr(it, 'articulo', None)
                if art_obj is None:
                    art_obj = Articulo.query.get(it.articulo_id)

                nombre_art = art_obj.Articulo_Nombre if art_obj else f'ID {it.articulo_id}'
                precio_unit = float((art_obj.Articulo_PrecioVenta if art_obj else 0) or 0)
                cant = int(it.cantidad or 0)
                subtotal = precio_unit * cant

                tn_val = getattr(it, 'talla_numero', None)
                tn_val = tn_val if tn_val not in (None, '') else None

                tipo_raw = (art_obj.Articulo_TipoTalla if art_obj else None) or 'ninguno'
                var_tipo = str(tipo_raw).strip().lower()
                if var_tipo not in ('talla', 'numero'):
                    var_tipo = 'ninguno'

                total_lista += subtotal
                total_cantidades += cant

                items.append({
                    'articulo_id': it.articulo_id,
                    'nombre': nombre_art,
                    'cantidad': cant,
                    'precio_unit': round(precio_unit, 2),
                    'subtotal': round(subtotal, 2),
                    'var_nombre': tn_val,
                    'var_tipo': var_tipo,
                })

            desc_tipo = (p.descuento_tipo or 'ninguno').lower()
            desc_val = float(p.descuento_valor or 0)

            if desc_tipo == 'porcentaje':
                desc_monto = round(total_lista * (desc_val / 100.0), 2)
            elif desc_tipo == 'monto':
                desc_monto = round(min(total_lista, desc_val), 2)
            else:
                desc_monto = 0.0

            total_final = round(total_lista - desc_monto, 2)

            paquetes.append({
                'id': p.id,
                'nombre': p.nombre,
                'descuento_tipo': desc_tipo,
                'descuento_valor': desc_val,
                'total_lista': round(total_lista, 2),
                'descuento_monto': desc_monto,
                'total_final': total_final,
                'detalle_items': items,
                'num_items': total_cantidades,
                'lineas': len(items_rel),
            })

        # Ordenamiento de paquetes
        if orden_paq == 'total':
            paquetes.sort(key=lambda x: (x['total_final'], x['nombre'].lower()))
        elif orden_paq == 'num_items':
            paquetes.sort(key=lambda x: (-x['num_items'], x['nombre'].lower()))
        else:
            paquetes.sort(key=lambda x: x['nombre'].lower())

    except Exception as e:
        app.logger.error(f"Error en consulta_articulos (paquetes): {str(e)}")
        flash('Ocurrió un error al obtener los paquetes', 'danger')

    return render_template(
        'consulta_articulos.html',
        articulos=articulos,
        paquetes=paquetes,
        orden=orden,
        orden_paq=orden_paq,
        busqueda=busqueda,
        vista=vista
    )



## Editar articulos
##----------------------------------------
@app.route('/editar_articulo/<int:id>', methods=['GET', 'POST'])
def editar_articulo(id):
    articulo = Articulo.query.get_or_404(id)
    form = ArticuloForm(obj=articulo)  # Esto carga automáticamente los campos coincidentes
    
    if request.method == 'GET':
        # Asegurarnos de cargar manualmente los campos que no coinciden exactamente
        form.nombre.data = articulo.Articulo_Nombre
        form.precio.data = float(articulo.Articulo_PrecioVenta)  # Convertir Decimal a float
        form.tipo_talla.data = articulo.Articulo_TipoTalla if articulo.Articulo_TipoTalla else 'ninguno'
        
        # Si no tiene tallas, establecer la existencia general
        if not articulo.Articulo_Tallas:
            form.existencia.data = articulo.Articulo_Existencia
        
        # Si tiene tallas, cargarlas en el FieldList
        if articulo.Articulo_Tallas:
            tallas = json.loads(articulo.Articulo_Tallas)
            # Limpiar cualquier entrada existente
            while len(form.tallas_numeros) > 0:
                form.tallas_numeros.pop_entry()
            
            # Agregar cada talla/número al formulario
            for nombre, cantidad in tallas.items():
                talla_form = form.tallas_numeros.append_entry()
                talla_form.nombre.data = nombre
                talla_form.cantidad.data = cantidad

    if form.validate_on_submit():
        try:
            # Procesar las tallas si aplica
            tallas = {}
            if form.tipo_talla.data != 'ninguno':
                for item in form.tallas_numeros.data:
                    if item['nombre'] and item['cantidad']:
                        tallas[item['nombre']] = item['cantidad']

            # Actualizar el artículo
            articulo.Articulo_Nombre = form.nombre.data
            articulo.Articulo_PrecioVenta = form.precio.data
            articulo.Articulo_Existencia = form.existencia.data if form.tipo_talla.data == 'ninguno' else sum(tallas.values())
            articulo.Articulo_TipoTalla = form.tipo_talla.data if form.tipo_talla.data != 'ninguno' else None
            articulo.Articulo_Tallas = json.dumps(tallas) if tallas else None

            db.session.commit()
            flash('Artículo actualizado exitosamente!', 'success')
            return redirect(url_for('consulta_articulos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error al actualizar artículo: {str(e)}', 'danger')
            app.logger.error(f"Error en editar_articulo: {str(e)}")
    
    return render_template('editar_articulo.html', form=form, articulo=articulo)

## Eliminar articulos
##----------------------------------------
@app.route('/eliminar_variante/<int:id>', methods=['POST'])
def eliminar_variante(id):
    if request.method == 'POST':
        articulo = Articulo.query.get_or_404(id)
        talla_a_eliminar = request.form.get('talla_numero')
        
        try:
            # Si es una variante con talla/número
            if talla_a_eliminar and talla_a_eliminar != '-':
                if articulo.eliminar_talla(talla_a_eliminar):
                    db.session.commit()
                    flash(f'Se eliminó la variante {talla_a_eliminar} correctamente', 'success')
                else:
                    flash('No se encontró la variante especificada', 'warning')
            # Si es un artículo sin tallas
            else:
                db.session.delete(articulo)
                db.session.commit()
                flash('Artículo eliminado completamente', 'success')
                
        except Exception as e:
            db.session.rollback()
            flash(f'Error al eliminar: {str(e)}', 'danger')
        
        return redirect(url_for('consulta_articulos'))


##---------------------------------------
## Paquetes
##----------------------------------------

## Registro Paquetes
##----------------------------------------
@app.route('/registro/paquete', methods=['GET', 'POST'])
def registro_paquete():
    import json, re
    form = PaqueteForm()

    # === 1) Cargar artículos desde BD y preparar metadatos ===
    arts = Articulo.query.order_by(Articulo.Articulo_Nombre.asc()).all()

    def _pk(a):
        return getattr(a, 'id', getattr(a, 'Articulo_ID', None))

    def _precio(a):
        return float(a.Articulo_PrecioVenta or 0)

    def _tipo(a):
        # 'ninguno' | 'talla' | 'numero' (None → 'ninguno')
        return a.Articulo_TipoTalla or 'ninguno'

    def _variantes(a):
        """Obtiene las tallas/números válidos desde Articulo_Tallas (JSON).
           Si es dict -> usa las claves; si es list -> lo convierte a str; si no hay -> []."""
        if not a.Articulo_Tallas:
            return []
        try:
            d = json.loads(a.Articulo_Tallas)
            if isinstance(d, dict):
                return list(d.keys())
            if isinstance(d, list):
                return [str(x) for x in d]
        except Exception:
            pass
        return []

    arts_data = []
    for a in arts:
        pk = _pk(a)
        if pk is None:
            continue
        arts_data.append({
            'pk': int(pk),
            'nombre': a.Articulo_Nombre,
            'precio': _precio(a),
            'tipo_talla': _tipo(a),       # 'ninguno'|'talla'|'numero'
            'variantes': _variantes(a),   # ej. ['CH','M','G'] o ['24','25',...]
        })

    art_choices = [(ad['pk'], ad['nombre']) for ad in arts_data]
    by_pk = {ad['pk']: ad for ad in arts_data}

    # Carga choices del SelectField de cada renglón existente
    for it in form.items:
        it.articulo.choices = art_choices

    # GET inicial → al menos un renglón
    if request.method == 'GET' and len(form.items) == 0:
        form.items.append_entry()
        form.items[0].articulo.choices = art_choices

    # === 2) Helpers para leer tallas/números desde request.form (sin depender de WTForms) ===
    # NOTA: el JS del template sigue creando campos:
    #   items-{i}-variantes-{j}-nombre   (talla o número seleccionado)
    #   items-{i}-variantes-{j}-cantidad (siempre 1 en tu UI actual)
    VAR_NOMBRE_RE = re.compile(r"^items-(\d+)-variantes-(\d+)-nombre$")
    VAR_CANT_RE   = re.compile(r"^items-(\d+)-variantes-(\d+)-cantidad$")

    def _collect_item_indices_from_request():
        """Encuentra los índices i de items presentes en el POST,
           buscando keys como items-{i}-articulo o items-{i}-cantidad."""
        indices = set()
        for k in request.form.keys():
            m = re.match(r"^items-(\d+)-(articulo|cantidad)$", k)
            if m:
                indices.add(int(m.group(1)))
        return sorted(indices)

    def _read_variants_for_item(i):
        """Lee todas las filas de talla/número para el item i y regresa
           una lista [{'nombre': 'M', 'cantidad': 1}, ...]"""
        nombres = {}
        cantidades = {}
        for k, v in request.form.items():
            m_nom = VAR_NOMBRE_RE.match(k)
            if m_nom and int(m_nom.group(1)) == i:
                vIdx = int(m_nom.group(2))
                nombres[vIdx] = (v or "").strip()
            m_can = VAR_CANT_RE.match(k)
            if m_can and int(m_can.group(1)) == i:
                vIdx = int(m_can.group(2))
                try:
                    cantidades[vIdx] = int(v or 0)
                except:
                    cantidades[vIdx] = 0
        out = []
        for vIdx in sorted(set(list(nombres.keys()) + list(cantidades.keys()))):
            out.append({
                'nombre': nombres.get(vIdx, ""),
                'cantidad': cantidades.get(vIdx, 0),
            })
        return out

    # === 3) POST: validar y guardar (solo estructura y tallas/números) ===
    if form.validate_on_submit():
        try:
            # Guardaremos tuplas: (art_id, cantidad, talla_numero(str|None), tipo_talla)
            resolved_rows = []

            # Indices de items
            if form.items.entries:
                item_indices = list(range(len(form.items.entries)))
            else:
                item_indices = _collect_item_indices_from_request()

            if not item_indices:
                raise ValueError('Agrega al menos un artículo al paquete.')

            for i in item_indices:
                # Obtener art_id y cantidad 1 (tu UI fija 1 por renglón)
                if i < len(form.items.entries):
                    val = form.items.entries[i].form.articulo.data
                    if hasattr(val, 'id') or hasattr(val, 'Articulo_ID'):
                        art_id = getattr(val, 'id', getattr(val, 'Articulo_ID', None))
                    else:
                        art_id = int(val) if val not in (None, '') else None

                    cantidad_general = form.items.entries[i].form.cantidad.data
                    try:
                        cantidad_general = int(cantidad_general or 0)
                    except:
                        cantidad_general = 0
                else:
                    art_raw = request.form.get(f'items-{i}-articulo')
                    art_id = int(art_raw) if art_raw not in (None, '') else None

                    q_raw = request.form.get(f'items-{i}-cantidad')
                    try:
                        cantidad_general = int(q_raw or 0)
                    except:
                        cantidad_general = 0

                if not art_id:
                    continue

                cfg = by_pk.get(int(art_id))
                if not cfg:
                    raise ValueError(f'Artículo con id {art_id} no existe.')

                tipo = cfg['tipo_talla']                   # 'ninguno'|'talla'|'numero'
                valores_validos = set(cfg['variantes'] or [])  # catálogo permitidos

                if tipo == 'ninguno':
                    if cantidad_general < 1:
                        raise ValueError('Cada cantidad debe ser ≥ 1.')
                    # Sin talla/número
                    resolved_rows.append((int(art_id), cantidad_general, None, 'ninguno'))
                else:
                    filas = _read_variants_for_item(i)
                    if not filas:
                        raise ValueError(f'Agrega al menos una {tipo} para "{cfg["nombre"]}".')

                    total_item = 0
                    for fila in filas:
                        talla_numero = (fila.get('nombre') or "").strip()
                        cant = int(fila.get('cantidad') or 0)

                        if not talla_numero:
                            raise ValueError(f'Debes elegir la {tipo} en todas las filas.')
                        if cant < 1:
                            raise ValueError('La cantidad por talla/número debe ser ≥ 1.')
                        if valores_validos and talla_numero not in valores_validos:
                            raise ValueError(f'La {tipo} "{talla_numero}" no es válida para "{cfg["nombre"]}".')

                        resolved_rows.append((int(art_id), cant, talla_numero, tipo))
                        total_item += cant

                    if total_item == 0:
                        raise ValueError(f'Agrega al menos una {tipo} con cantidad ≥ 1 para "{cfg["nombre"]}".')

            if not resolved_rows:
                raise ValueError('Agrega al menos un artículo válido.')

            # Guardado del paquete (sin validar existencias)
            nuevo = Paquete(
                nombre=form.nombre.data.strip(),
                descuento_tipo=form.descuento_tipo.data,
                descuento_valor=form.descuento_valor.data or 0
            )
            db.session.add(nuevo)
            db.session.flush()

            # Guardar líneas: SOLO escribimos talla_numero (si aplica)
            for art_id, cant, talla_numero, tipo_talla in resolved_rows:
                item = PaqueteItem(
                    paquete_id=nuevo.id,
                    articulo_id=int(art_id),
                    cantidad=int(cant)
                )
                # Si el artículo maneja talla/numero, guardamos el valor seleccionado
                if tipo_talla in ('talla', 'numero'):
                    item.talla_numero = talla_numero  # "CH", "M", "25", etc.

                db.session.add(item)

            db.session.commit()
            flash('Paquete registrado exitosamente.', 'success')
            return redirect(url_for('registro_paquete'))

        except Exception as e:
            db.session.rollback()
            current_app.logger.exception('Error al registrar paquete')
            flash(f'Error al registrar paquete: {e}', 'danger')

    elif request.method == 'POST':
        flash('Por favor corrige los errores del formulario.', 'warning')

    # Reinyectar choices antes de renderizar
    for it in form.items:
        it.articulo.choices = art_choices

    return render_template('registro_paquete.html',
                           form=form,
                           arts_data=arts_data)  # [{pk,nombre,precio,tipo_talla,variantes:[...]}]

## Editar Paquetes
##----------------------------------------
@app.route('/paquetes/<int:paquete_id>/editar', methods=['GET', 'POST'])
def editar_paquete(paquete_id):
    import json, re
    from sqlalchemy.orm import joinedload

    # -------- helpers comunes (idénticos a registro) --------
    def _pk(a):
        return getattr(a, 'id', getattr(a, 'Articulo_ID', None))

    def _precio(a):
        return float(a.Articulo_PrecioVenta or 0)

    def _tipo(a):
        # 'ninguno' | 'talla' | 'numero' (None → 'ninguno')
        return a.Articulo_TipoTalla or 'ninguno'

    def _variantes(a):
        """Obtiene tallas/números válidos desde Articulo_Tallas (JSON)."""
        if not a.Articulo_Tallas:
            return []
        try:
            d = json.loads(a.Articulo_Tallas)
            if isinstance(d, dict):
                return list(d.keys())
            if isinstance(d, list):
                return [str(x) for x in d]
        except Exception:
            pass
        return []

    # -------- obtener paquete ----------
    paq = (Paquete.query
           .options(joinedload(Paquete.items).joinedload(PaqueteItem.articulo))
           .get_or_404(paquete_id))

    form = PaqueteForm()

    # -------- catálogo de artículos para el selector ----------
    arts = Articulo.query.order_by(Articulo.Articulo_Nombre.asc()).all()
    arts_data = []
    for a in arts:
        pk = _pk(a)
        if pk is None:
            continue
        arts_data.append({
            'pk': int(pk),
            'nombre': a.Articulo_Nombre,
            'precio': _precio(a),
            'tipo_talla': _tipo(a),       # 'ninguno'|'talla'|'numero'
            'variantes': _variantes(a),   # ej. ['CH','M','G'] o ['24','25',...]
        })
    art_choices = [(ad['pk'], ad['nombre']) for ad in arts_data]
    by_pk = {ad['pk']: ad for ad in arts_data}

    # Cargar choices en las entradas existentes del FieldList
    for it in form.items:
        it.articulo.choices = art_choices

    # -------- regex y lectores del POST (idénticos a registro) ----------
    VAR_NOMBRE_RE = re.compile(r"^items-(\d+)-variantes-(\d+)-nombre$")
    VAR_CANT_RE   = re.compile(r"^items-(\d+)-variantes-(\d+)-cantidad$")

    def _collect_item_indices_from_request():
        indices = set()
        for k in request.form.keys():
            m = re.match(r"^items-(\d+)-(articulo|cantidad)$", k)
            if m:
                indices.add(int(m.group(1)))
        return sorted(indices)

    def _read_variants_for_item(i):
        nombres = {}
        cantidades = {}
        for k, v in request.form.items():
            m_nom = VAR_NOMBRE_RE.match(k)
            if m_nom and int(m_nom.group(1)) == i:
                vIdx = int(m_nom.group(2))
                nombres[vIdx] = (v or "").strip()
            m_can = VAR_CANT_RE.match(k)
            if m_can and int(m_can.group(1)) == i:
                vIdx = int(m_can.group(2))
                try:
                    cantidades[vIdx] = int(v or 0)
                except:
                    cantidades[vIdx] = 0
        out = []
        for vIdx in sorted(set(list(nombres.keys()) + list(cantidades.keys()))):
            out.append({
                'nombre': nombres.get(vIdx, ""),
                'cantidad': cantidades.get(vIdx, 0),
            })
        return out

    # -------- POST: actualizar paquete ----------
    if form.validate_on_submit():
        try:
            # 1) Resolver filas (igual que en registro)
            resolved_rows = []  # (art_id, cantidad, talla_numero(str|None), tipo_talla)

            if form.items.entries:
                item_indices = list(range(len(form.items.entries)))
            else:
                item_indices = _collect_item_indices_from_request()

            if not item_indices:
                raise ValueError('Agrega al menos un artículo al paquete.')

            for i in item_indices:
                # Obtener art_id y cantidad_general (tu UI: 1 por renglón)
                if i < len(form.items.entries):
                    val = form.items.entries[i].form.articulo.data
                    if hasattr(val, 'id') or hasattr(val, 'Articulo_ID'):
                        art_id = getattr(val, 'id', getattr(val, 'Articulo_ID', None))
                    else:
                        art_id = int(val) if val not in (None, '') else None

                    cantidad_general = form.items.entries[i].form.cantidad.data
                    try:
                        cantidad_general = int(cantidad_general or 0)
                    except:
                        cantidad_general = 0
                else:
                    art_raw = request.form.get(f'items-{i}-articulo')
                    art_id = int(art_raw) if art_raw not in (None, '') else None

                    q_raw = request.form.get(f'items-{i}-cantidad')
                    try:
                        cantidad_general = int(q_raw or 0)
                    except:
                        cantidad_general = 0

                if not art_id:
                    continue

                cfg = by_pk.get(int(art_id))
                if not cfg:
                    raise ValueError(f'Artículo con id {art_id} no existe.')

                tipo = cfg['tipo_talla']                   # 'ninguno'|'talla'|'numero'
                valores_validos = set(cfg['variantes'] or [])

                if tipo == 'ninguno':
                    if cantidad_general < 1:
                        raise ValueError('Cada cantidad debe ser ≥ 1.')
                    resolved_rows.append((int(art_id), cantidad_general, None, 'ninguno'))
                else:
                    filas = _read_variants_for_item(i)
                    if not filas:
                        raise ValueError(f'Agrega al menos una {tipo} para "{cfg["nombre"]}".')

                    total_item = 0
                    for fila in filas:
                        tn = (fila.get('nombre') or "").strip()
                        cant = int(fila.get('cantidad') or 0)
                        if not tn:
                            raise ValueError(f'Debes elegir la {tipo} en todas las filas.')
                        if cant < 1:
                            raise ValueError('La cantidad por talla/número debe ser ≥ 1.')
                        if valores_validos and tn not in valores_validos:
                            raise ValueError(f'La {tipo} "{tn}" no es válida para "{cfg["nombre"]}".')

                        resolved_rows.append((int(art_id), cant, tn, tipo))
                        total_item += cant

                    if total_item == 0:
                        raise ValueError(f'Agrega al menos una {tipo} con cantidad ≥ 1 para "{cfg["nombre"]}".')

            if not resolved_rows:
                raise ValueError('Agrega al menos un artículo válido.')

            # 2) Actualizar encabezado del paquete
            paq.nombre = form.nombre.data.strip()
            paq.descuento_tipo = form.descuento_tipo.data
            paq.descuento_valor = form.descuento_valor.data or 0

            # 3) Reemplazar líneas existentes por las nuevas
            #    (delete-orphan hace el trabajo al limpiar la relación)
            paq.items.clear()
            db.session.flush()

            for art_id, cant, tn, tipo_talla in resolved_rows:
                item = PaqueteItem(
                    paquete_id=paq.id,
                    articulo_id=int(art_id),
                    cantidad=int(cant)
                )
                if tipo_talla in ('talla', 'numero'):
                    item.talla_numero = tn
                db.session.add(item)

            db.session.commit()
            flash('Paquete actualizado exitosamente.', 'success')
            return redirect(url_for('consulta_articulos', vista='paquetes'))

        except Exception as e:
            db.session.rollback()
            current_app.logger.exception('Error al actualizar paquete')
            flash(f'Error al actualizar paquete: {e}', 'danger')

    # -------- GET: precargar formulario e info para el front ----------
    else:
        # Prefill de encabezado
        form.nombre.data = paq.nombre
        form.descuento_tipo.data = paq.descuento_tipo
        form.descuento_valor.data = paq.descuento_valor

        # Asegurar al menos tantas entradas como líneas existan
        lineas = paq.items or []
        faltan = max(1, len(lineas)) - len(form.items)
        for _ in range(faltan):
            form.items.append_entry()

        # Cargar choices del select de cada renglón
        for it in form.items:
            it.articulo.choices = art_choices

        # Construir arreglo de prellenado para el JS del template
        prefill_items = []
        for idx, it in enumerate(lineas):
            art_obj = it.articulo or Articulo.query.get(it.articulo_id)
            tipo = (art_obj.Articulo_TipoTalla if art_obj else None) or 'ninguno'
            prefill_items.append({
                'idx': idx,
                'articulo_id': it.articulo_id,
                'cantidad': int(it.cantidad or 1),
                'tipo_talla': str(tipo).lower(),
                'talla_numero': it.talla_numero or '',
            })

        # SUGERENCIA: en tu template (editar), añade un bloque JS que:
        # - Setee select de artículo por cada fila
        # - Llame a la misma lógica de UI para crear filas de talla/número
        # - Seleccione la opción guardada (talla_numero)
        # Ejemplo (pseudocódigo a insertar en el template):
        #
        # const prefill = {{ prefill_items|tojson }};
        # prefill.forEach(row => {
        #   const card = document.querySelectorAll('.paquete-item')[row.idx];
        #   const sel = card.querySelector('.articulo-select');
        #   sel.value = String(row.articulo_id);
        #   sel.dispatchEvent(new Event('change')); // para que pinte tn-wrapper
        #   if (row.tipo_talla === 'talla' || row.tipo_talla === 'numero') {
        #       const tbody = card.querySelector('.tn-table tbody');
        #       // crea 1 fila y selecciona la talla/número:
        #       const addBtn = card.querySelector('.btn-add-tn');
        #       addBtn.click();
        #       const lastSel = tbody.querySelector('.tn-row:last-child select.tn-nombre');
        #       if (lastSel) { lastSel.value = row.talla_numero; }
        #   }
        # });

        return render_template('registro_paquete.html',
                               form=form,
                               arts_data=arts_data,
                               edit_mode=True,
                               paquete_id=paq.id,
                               prefill_items=prefill_items)




## Eliminar Paquetes
##----------------------------------------
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

@app.route('/paquetes/<int:paquete_id>/eliminar', methods=['POST'])
def eliminar_paquete(paquete_id):
    try:
        paq = Paquete.query.get_or_404(paquete_id)

        # Al eliminar el paquete, SQLAlchemy eliminará sus items por el cascade
        db.session.delete(paq)
        db.session.commit()
        flash('Paquete eliminado exitosamente.', 'success')

    except IntegrityError:
        db.session.rollback()
        current_app.logger.exception('Integridad referencial al eliminar paquete')
        flash('No se puede eliminar el paquete porque está referenciado por otros registros.', 'danger')

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('Error al eliminar paquete')
        flash(f'Error al eliminar paquete: {e}', 'danger')

    return redirect(url_for('consulta_articulos', vista='paquetes'))




# --------------------------------------------------
# Pagos
# --------------------------------------------------

## Agregar Pagos
##----------------------------------------
@app.route('/registro_pago', methods=['GET', 'POST'])
def registro_pago():
    form = PagoForm()
    if form.validate_on_submit():
        try:
            # Descuento
            condiciones = []
            if form.aplicar_descuento.data:
                if form.condicion_efectivo.data: condiciones.append('efectivo')
                if form.condicion_tarjeta.data: condiciones.append('tarjeta')
                if form.condicion_transferencia.data: condiciones.append('transferencia')
                if form.condicion_deposito.data: condiciones.append('deposito')

            # Recargo
            tiene_recargo = bool(getattr(form, 'aplicar_recargo', False) and form.aplicar_recargo.data)
            recargo_pct = form.porcentaje_recargo.data if tiene_recargo else None
            recargo_dia_mes = form.recargo_dia_mes.data if (tiene_recargo and form.es_mensual.data) else None
            recargo_fecha = form.recargo_fecha.data if (tiene_recargo and not form.es_mensual.data) else None

            # Expiración (solo pagos únicos)
            tiene_expiracion = bool(form.aplicar_expiracion.data)
            expira_fecha = form.expira_fecha.data if (tiene_expiracion and not form.es_mensual.data) else None

            nuevo_pago = Pago(
                Pago_Monto=form.monto.data,
                Pago_Tipo=form.tipo_pago.data.strip(),

                # Periodicidad
                Pago_Es_Mensual=form.es_mensual.data,

                # Descuento
                Pago_Descuento_Tipo=(form.nombre_descuento.data if form.aplicar_descuento.data else None),
                Pago_Descuento_Porcentaje=(form.porcentaje_descuento.data if form.aplicar_descuento.data else None),
                Pago_Condiciones=(json.dumps(condiciones) if condiciones else None),
                # Válido hasta solo si NO es mensual
                Pago_Restricciones_Fecha=(form.restricciones_fecha.data
                                          if form.aplicar_descuento.data and not form.es_mensual.data else None),

                # Recargo
                Pago_Tiene_Recargo=tiene_recargo,
                Pago_Recargo_Porcentaje=recargo_pct,
                Pago_Recargo_DiaMes=recargo_dia_mes,
                Pago_Recargo_Fecha=recargo_fecha,

                # Expiración (pagos únicos)
                Pago_Tiene_Expiracion=tiene_expiracion,
                Pago_Expira_Fecha=expira_fecha,

                Est_ID=None
            )

            db.session.add(nuevo_pago)
            db.session.commit()
            flash('✅ Pago registrado exitosamente!', 'success')
            return redirect(url_for('index'))

        except IntegrityError:
            db.session.rollback()
            flash('❌ Error: Problema con la base de datos. Contacte al administrador.', 'danger')
        except Exception as e:
            db.session.rollback()
            flash(f'❌ Error inesperado: {str(e)}', 'danger')

    return render_template('registro_pago.html', form=form)



# -------------------------------
# CONSULTA DE PAGOS
# -------------------------------
@app.route('/consulta_pagos', methods=['GET'])
def consulta_pagos():
    from sqlalchemy import or_
    from datetime import date

    # Parámetros de búsqueda y ordenamiento
    busqueda = request.args.get('busqueda', '').strip()
    orden = request.args.get('orden', 'fecha')
    direccion = request.args.get('dir', 'desc')

    # Construir consulta base
    query = Pago.query

    # Aplicar filtro de búsqueda (tipo, descuento, condiciones, periodicidad, recargo, expiración)
    if busqueda:
        b = busqueda.lower()

        # Texto libre (en tipo/desc/condiciones)
        query = query.filter(
            or_(
                Pago.Pago_Tipo.ilike(f'%{busqueda}%'),
                Pago.Pago_Descuento_Tipo.ilike(f'%{busqueda}%'),
                Pago.Pago_Condiciones.ilike(f'%{busqueda}%')
            )
        )

        # Periodicidad
        if 'mensual' in b:
            query = query.filter(Pago.Pago_Es_Mensual.is_(True))
        elif 'unico' in b or 'único' in b:
            query = query.filter(Pago.Pago_Es_Mensual.is_(False))

        # Recargo: "recargo" => con recargo; "sin recargo"/"no recargo" => sin recargo
        if 'recargo' in b:
            if 'sin recargo' in b or 'no recargo' in b or 'sin' in b and 'recargo' in b:
                query = query.filter(Pago.Pago_Tiene_Recargo.is_(False))
            else:
                query = query.filter(Pago.Pago_Tiene_Recargo.is_(True))

        # Expiración (solo para pagos únicos)
        today = date.today()
        if 'expirado' in b:
            query = query.filter(
                Pago.Pago_Es_Mensual.is_(False),
                Pago.Pago_Tiene_Expiracion.is_(True),
                Pago.Pago_Expira_Fecha.isnot(None),
                Pago.Pago_Expira_Fecha < today
            )
        elif 'vigente' in b:
            query = query.filter(
                Pago.Pago_Es_Mensual.is_(False),
                Pago.Pago_Tiene_Expiracion.is_(True),
                Pago.Pago_Expira_Fecha.isnot(None),
                Pago.Pago_Expira_Fecha >= today
            )
        elif 'sin expiracion' in b or 'sin expiración' in b or 'no expiracion' in b or 'no expiración' in b:
            query = query.filter(Pago.Pago_Tiene_Expiracion.is_(False))

    # Ordenamiento
    if orden == 'tipo':
        order_field = Pago.Pago_Tipo
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    elif orden == 'descuento':
        order_field = Pago.Pago_Descuento_Tipo
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    elif orden == 'condiciones':
        order_field = Pago.Pago_Condiciones
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    elif orden == 'validez':
        order_field = Pago.Pago_Restricciones_Fecha
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    elif orden == 'periodicidad':
        order_field = Pago.Pago_Es_Mensual
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    elif orden == 'recargo':
        # Primero por si tiene recargo, luego por porcentaje
        if direccion == 'desc':
            query = query.order_by(Pago.Pago_Tiene_Recargo.desc(), Pago.Pago_Recargo_Porcentaje.desc())
        else:
            query = query.order_by(Pago.Pago_Tiene_Recargo.asc(), Pago.Pago_Recargo_Porcentaje.asc())
    elif orden == 'expira':
        order_field = Pago.Pago_Expira_Fecha
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())
    else:  # fecha (por defecto)
        order_field = Pago.Pago_Fecha
        query = query.order_by(order_field.desc() if direccion == 'desc' else order_field.asc())

    pagos = query.all()

    return render_template(
        'consulta_pagos.html',
        pagos=pagos,
        busqueda=busqueda,
        orden=orden,
        direccion=direccion
    )



# -------------------------------
# EDITAR PAGO
# -------------------------------
@app.route('/editar_pago/<int:id>', methods=['GET', 'POST'])
def editar_pago(id):
    pago = Pago.query.get_or_404(id)
    form = PagoForm()

    # Cargar datos existentes al formulario
    if request.method == 'GET':
        form.tipo_pago.data = pago.Pago_Tipo
        form.monto.data = float(pago.Pago_Monto)

        # Periodicidad
        form.es_mensual.data = bool(pago.Pago_Es_Mensual)

        # Descuento
        if pago.Pago_Descuento_Tipo:
            form.aplicar_descuento.data = True
            form.nombre_descuento.data = pago.Pago_Descuento_Tipo
            form.porcentaje_descuento.data = (
                float(pago.Pago_Descuento_Porcentaje)
                if pago.Pago_Descuento_Porcentaje is not None else None
            )

            # Condiciones
            if pago.Pago_Condiciones:
                try:
                    condiciones = json.loads(pago.Pago_Condiciones)
                except Exception:
                    condiciones = []
                condiciones_l = [str(c).strip().lower() for c in condiciones]
                form.condicion_efectivo.data = 'efectivo' in condiciones_l
                form.condicion_tarjeta.data = 'tarjeta' in condiciones_l
                form.condicion_transferencia.data = 'transferencia' in condiciones_l
                form.condicion_deposito.data = 'deposito' in condiciones_l

            # Fecha de restricción (solo si NO es mensual)
            form.restricciones_fecha.data = (pago.Pago_Restricciones_Fecha
                                             if not pago.Pago_Es_Mensual else None)

        # Recargo
        form.aplicar_recargo.data = bool(pago.Pago_Tiene_Recargo)
        form.porcentaje_recargo.data = (
            float(pago.Pago_Recargo_Porcentaje)
            if pago.Pago_Recargo_Porcentaje is not None else None
        )

        if pago.Pago_Es_Mensual:
            form.recargo_dia_mes.data = pago.Pago_Recargo_DiaMes
            form.recargo_fecha.data = None
        else:
            form.recargo_dia_mes.data = None
            form.recargo_fecha.data = pago.Pago_Recargo_Fecha

        # Expiración (solo si NO es mensual)
        form.aplicar_expiracion.data = bool(pago.Pago_Tiene_Expiracion) and not pago.Pago_Es_Mensual
        form.expira_fecha.data = (pago.Pago_Expira_Fecha if (not pago.Pago_Es_Mensual and pago.Pago_Tiene_Expiracion) else None)

    if form.validate_on_submit():
        try:
            # Básicos
            pago.Pago_Tipo = form.tipo_pago.data.strip()
            pago.Pago_Monto = form.monto.data
            pago.Pago_Es_Mensual = bool(form.es_mensual.data)

            # Descuento
            if form.aplicar_descuento.data:
                pago.Pago_Descuento_Tipo = form.nombre_descuento.data
                pago.Pago_Descuento_Porcentaje = form.porcentaje_descuento.data

                condiciones = []
                if form.condicion_efectivo.data: condiciones.append('efectivo')
                if form.condicion_tarjeta.data: condiciones.append('tarjeta')
                if form.condicion_transferencia.data: condiciones.append('transferencia')
                if form.condicion_deposito.data: condiciones.append('deposito')
                pago.Pago_Condiciones = json.dumps(condiciones) if condiciones else None

                # Válido hasta: solo si NO es mensual
                pago.Pago_Restricciones_Fecha = (
                    form.restricciones_fecha.data if not form.es_mensual.data else None
                )
            else:
                pago.Pago_Descuento_Tipo = None
                pago.Pago_Descuento_Porcentaje = None
                pago.Pago_Condiciones = None
                pago.Pago_Restricciones_Fecha = None

            # Recargo
            if form.aplicar_recargo.data:
                pago.Pago_Tiene_Recargo = True
                pago.Pago_Recargo_Porcentaje = form.porcentaje_recargo.data

                if form.es_mensual.data:
                    pago.Pago_Recargo_DiaMes = form.recargo_dia_mes.data
                    pago.Pago_Recargo_Fecha = None
                else:
                    pago.Pago_Recargo_Fecha = form.recargo_fecha.data
                    pago.Pago_Recargo_DiaMes = None
            else:
                pago.Pago_Tiene_Recargo = False
                pago.Pago_Recargo_Porcentaje = None
                pago.Pago_Recargo_DiaMes = None
                pago.Pago_Recargo_Fecha = None

            # Expiración (solo pagos únicos)
            if form.aplicar_expiracion.data and not form.es_mensual.data:
                pago.Pago_Tiene_Expiracion = True
                pago.Pago_Expira_Fecha = form.expira_fecha.data
            else:
                pago.Pago_Tiene_Expiracion = False
                pago.Pago_Expira_Fecha = None

            db.session.commit()
            flash('✅ Pago actualizado exitosamente!', 'success')
            return redirect(url_for('consulta_pagos'))

        except Exception as e:
            db.session.rollback()
            flash(f'❌ Error al actualizar el pago: {str(e)}', 'danger')

    return render_template('editar_pago.html', form=form, pago=pago)

## Eliminar pago
##----------------------------------------
@app.route('/eliminar_pago/<int:id>', methods=['POST'])
def eliminar_pago(id):
    pago = Pago.query.get_or_404(id)
    try:
        db.session.delete(pago)
        db.session.commit()
        flash('✅ Pago eliminado exitosamente!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'❌ Error al eliminar el pago: {str(e)}', 'danger')
    return redirect(url_for('consulta_pagos'))


##----------------------------------------
## Venta
##----------------------------------------


# ----------------------------------------
# Registrar venta
# ----------------------------------------
@app.route('/registro/venta', methods=['GET', 'POST'])
def registro_venta():
    form = VentaForm()
    if request.method == 'POST':
        # Rehidrata WTForms con lo que viene del form
        form.process(request.form)

    # === Catálogos ===
    estudiantes = (Estudiante.query
                   .order_by(Estudiante.Est_Nombre.asc(), Estudiante.Est_ApellidoP.asc())
                   .all())
    instructores = (Instructor.query
                    .order_by(Instructor.Instructor_Nombre.asc(), Instructor.Instructor_ApellidoP.asc())
                    .all())

    # === Helpers JSON robustos ===
    def _safe_json_loads(s, default):
        if not s:
            return default
        try:
            return json.loads(s)
        except Exception:
            return default

    # === Artículos y variantes (robusto) ===
    KEY_SEP = ":::"
    articulos = Articulo.query.order_by(Articulo.Articulo_Nombre.asc()).all()
    variantes = []
    articulos_dict = {}

    for art in articulos:
        precio = float(art.Articulo_PrecioVenta or 0)
        tipo_raw = (art.Articulo_TipoTalla or 'ninguno').strip().lower()
        tallas_raw = _safe_json_loads(art.Articulo_Tallas, default={})

        # Lista -> variantes sin existencias por talla
        if isinstance(tallas_raw, list):
            for talla in tallas_raw:
                talla = str(talla)
                key = f"{art.Articulo_ID}{KEY_SEP}{talla}"
                variantes.append((key, f"{art.Articulo_Nombre} ({talla})"))
                articulos_dict[key] = {
                    "Articulo_ID": art.Articulo_ID,
                    "Articulo_Nombre": art.Articulo_Nombre,
                    "Talla": talla,
                    "Precio": precio,
                    "Existencia": int(getattr(art, "Articulo_Existencia", 0) or 0),
                    "Tipo_Talla": tipo_raw,
                }

        # Dict -> existencias por variante
        elif isinstance(tallas_raw, dict) and tallas_raw:
            for talla, existencia in tallas_raw.items():
                talla = str(talla)
                key = f"{art.Articulo_ID}{KEY_SEP}{talla}"
                variantes.append((key, f"{art.Articulo_Nombre} ({talla})"))
                articulos_dict[key] = {
                    "Articulo_ID": art.Articulo_ID,
                    "Articulo_Nombre": art.Articulo_Nombre,
                    "Talla": talla,
                    "Precio": precio,
                    "Existencia": int(existencia or 0),
                    "Tipo_Talla": tipo_raw,
                }

        # Sin tallas / JSON vacío o corrupto
        else:
            key = f"{art.Articulo_ID}"
            variantes.append((key, f"{art.Articulo_Nombre} (sin talla)"))
            articulos_dict[key] = {
                "Articulo_ID": art.Articulo_ID,
                "Articulo_Nombre": art.Articulo_Nombre,
                "Talla": None,
                "Precio": precio,
                "Existencia": int(getattr(art, "Articulo_Existencia", 0) or 0),
                "Tipo_Talla": 'ninguno',
            }

    # === Choices con placeholder 0 ===
    form.estudiante_id.choices = [(0, "— Selecciona —")] + [
        (e.Est_ID, f"{e.Est_Nombre} {e.Est_ApellidoP}") for e in estudiantes
    ]
    form.instructor_id.choices = [(0, "— Selecciona —")] + [
        (i.Instructor_ID, f"{i.Instructor_Nombre} {i.Instructor_ApellidoP}") for i in instructores
    ]
    form.articulos.choices = variantes

    # === Pagos (conceptos académicos) para select y JS ===
    pagos = Pago.query.order_by(Pago.Pago_Tipo.asc()).all()

    if hasattr(form, 'pagos'):
        form.pagos.choices = [
            (p.Pago_ID, f"{p.Pago_Tipo} - ${float(p.Pago_Monto or 0):.2f}")
            for p in pagos
        ]

    def _parse_conds(raw):
        """Devuelve lista de condiciones (minúsculas). Acepta JSON o texto plano separado por , ; |"""
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(c).strip().lower() for c in data if str(c).strip()]
            if isinstance(data, str):
                raw = data
        except Exception:
            pass
        parts = re.split(r'[,;|]+', str(raw))
        return [p.strip().lower() for p in parts if p.strip()]

    pagos_dict = {}
    for p in pagos:
        pagos_dict[p.Pago_ID] = {
            "monto": float(p.Pago_Monto or 0),
            "descuento_tipo": p.Pago_Descuento_Tipo,
            "descuento_porcentaje": float(p.Pago_Descuento_Porcentaje) if p.Pago_Descuento_Porcentaje is not None else 0.0,
            "condiciones": _parse_conds(p.Pago_Condiciones),
            "valido_hasta": p.Pago_Restricciones_Fecha.isoformat() if p.Pago_Restricciones_Fecha else None
        }

    # === Helpers método de pago / referencia ===
    def _normalize_method(val: str) -> str:
        v = (val or "").strip().lower()
        map_alias = {
            "cash": "efectivo",
            "tarjeta de crédito": "tarjeta",
            "tarjeta de debito": "tarjeta",
            "tarjeta de débito": "tarjeta",
            "debito": "tarjeta",
            "débito": "tarjeta",
            "transferencia bancaria": "transferencia",
            "depósito": "deposito",
            "deposito bancario": "deposito",
        }
        return map_alias.get(v, v)

    def _requires_reference(method_norm: str) -> bool:
        return method_norm in {"transferencia", "tarjeta", "deposito"}

    # === Paquetes (robusto) ===
    paquetes_q = (Paquete.query
                  .options(joinedload(Paquete.items).joinedload(PaqueteItem.articulo))
                  .order_by(Paquete.nombre.asc()))
    paquetes_db = paquetes_q.all()

    paquetes_choices = []
    paquetes_dict = {}
    for p in paquetes_db:
        items_rel = getattr(p, 'items', []) or []
        items = []
        total_lista = 0.0
        total_cant = 0

        for it in items_rel:
            art_obj = getattr(it, 'articulo', None)
            if art_obj is None:
                art_obj = db.session.get(Articulo, it.articulo_id)

            nombre_art = art_obj.Articulo_Nombre if art_obj else f'ID {it.articulo_id}'
            precio_unit = float((art_obj.Articulo_PrecioVenta if art_obj else 0) or 0)
            cant = int(it.cantidad or 0)
            subtotal = precio_unit * cant

            tn_val = getattr(it, 'talla_numero', None)
            tn_val = tn_val if tn_val not in (None, '') else None

            total_lista += subtotal
            total_cant += cant

            items.append({
                'articulo_id': it.articulo_id,
                'nombre': nombre_art,
                'cantidad': cant,
                'precio_unit': round(precio_unit, 2),
                'subtotal': round(subtotal, 2),
                'talla': tn_val,
            })

        desc_tipo = (p.descuento_tipo or 'ninguno').strip().lower()
        desc_val = float(p.descuento_valor or 0)
        if desc_tipo == 'porcentaje':
            desc_monto = round(total_lista * (desc_val / 100.0), 2)
        elif desc_tipo == 'monto':
            desc_monto = round(min(total_lista, desc_val), 2)
        else:
            desc_monto = 0.0

        total_final = round(total_lista - desc_monto, 2)
        pr = (total_final / total_lista) if total_lista > 0 else 1.0

        items_aj = []
        for it in items:
            pu_aj = round(it['precio_unit'] * pr, 2)
            items_aj.append({
                'articulo_id': it['articulo_id'],
                'talla': it['talla'],
                'cantidad': it['cantidad'],
                'precio_unit_ajustado': pu_aj,
                'precio_unit_lista': it['precio_unit'],
                'nombre': it['nombre'],
            })

        paquetes_choices.append((p.id, f"{p.nombre} ({len(items)} líneas, {total_cant} piezas) — ${total_final:.2f}"))
        paquetes_dict[p.id] = {
            'id': p.id,
            'nombre': p.nombre,
            'descuento_tipo': desc_tipo,
            'descuento_valor': desc_val,
            'total_lista': round(total_lista, 2),
            'descuento_monto': desc_monto,
            'total_final': total_final,
            'items': items_aj,
            'lineas': len(items),
            'piezas': total_cant,
            'prorrateo': pr,
        }

    if hasattr(form, 'paquetes'):
        form.paquetes.choices = [(pid, label) for pid, label in paquetes_choices]

    # === PENDIENTES ===
    pend_rows = (Venta.query
                 .options(
                     joinedload(Venta.estudiante),
                     joinedload(Venta.instructor),
                     selectinload(Venta.lineas),
                     selectinload(Venta.pagos)
                 )
                 .filter(
                     or_(
                         Venta.Metodo_Pago.is_(None),
                         Venta.Metodo_Pago == '',
                         func.lower(Venta.Metodo_Pago) == '__pendiente__',
                         func.lower(Venta.Metodo_Pago) == 'pendiente'
                     )
                 )
                 .order_by(Venta.Fecha_Venta.desc())
                 .all())

    pendientes = []
    for v in pend_rows:
        if getattr(v, 'Est_ID', None):
            cliente_tipo = 'estudiante'
            cliente_id = v.Est_ID
            cliente_nombre = (f"{v.estudiante.Est_Nombre} {v.estudiante.Est_ApellidoP}"
                              if getattr(v, 'estudiante', None) else '—')
        elif getattr(v, 'Instructor_ID', None):
            cliente_tipo = 'instructor'
            cliente_id = v.Instructor_ID
            cliente_nombre = (f"{v.instructor.Instructor_Nombre} {v.instructor.Instructor_ApellidoP}"
                              if getattr(v, 'instructor', None) else '—')
        else:
            cliente_tipo = 'desconocido'
            cliente_id = None
            cliente_nombre = '—'

        items = []
        for ln in v.lineas:
            key = f"{ln.Articulo_ID}{KEY_SEP}{ln.Talla}" if ln.Talla else f"{ln.Articulo_ID}"
            meta = articulos_dict.get(key)
            if meta:
                desc = meta["Articulo_Nombre"] + (f" ({meta['Talla']})" if meta["Talla"] else "")
                precio_u = float(meta["Precio"])
            else:
                art = db.session.get(Articulo, ln.Articulo_ID)
                nombre = getattr(art, 'Articulo_Nombre', f"Artículo {ln.Articulo_ID}")
                desc = nombre + (f" ({ln.Talla})" if ln.Talla else "")
                precio_u = float(getattr(ln, 'Precio_Unitario', 0) or 0)
            items.append({
                "Articulo_ID": ln.Articulo_ID,
                "Talla": ln.Talla,
                "Cantidad": int(ln.Cantidad or 0),
                "Precio": float(getattr(ln, 'Precio_Unitario', precio_u) or precio_u),
                "Descripcion": desc
            })

        pagos_list = []
        for p in getattr(v, 'pagos', []):
            pagos_list.append({
                "Pago_ID": p.Pago_ID,
                "Descripcion": p.Pago_Tipo,
                "Monto": float(p.Pago_Monto or 0.0)
            })

        pendientes.append({
            "Venta_ID": v.Venta_ID,
            "Fecha_Venta": v.Fecha_Venta.isoformat() if v.Fecha_Venta else None,
            "cliente_tipo": cliente_tipo,
            "cliente_id": cliente_id,
            "cliente_nombre": cliente_nombre,
            "lineas": items,
            "pagos": pagos_list,
            "Metodo_Pago": v.Metodo_Pago
        })

    # === Banderas de UI ===
    error_flags = {
        "cliente": False,
        "articulos_o_pagos": False,  # estudiante: artículos o pagos
        "articulos": False,          # instructor: artículos
        "metodo_pago": False,
        "referencia": False,
        "db": False,
        "unexpected": False,
        "abonos": False,             # NUEVO
    }

    # ===========================
    # Helpers de Plan/Abono/Liq
    # ===========================
    def _find_open_plan(est_id: int, tipo_item: str, item_ref_id: int):
        """Busca un plan ABIERTO por combinación (est, tipo, item)."""
        q = PlanCobro.query.filter(PlanCobro.Est_ID == est_id,
                                   PlanCobro.Estado == 'abierto')
        if tipo_item == 'articulo':
            q = q.filter(PlanCobro.Articulo_ID == item_ref_id)
        elif tipo_item == 'paquete':
            q = q.filter(PlanCobro.Paquete_ID == item_ref_id)
        else:
            q = q.filter(PlanCobro.Pago_ID == item_ref_id)
        return q.first()

    def _build_plan_snapshot(est_id: int, sub):
        """
        Obtiene descripcion y total para crear plan si no existe.
        Prefiere lo que venga del modal; si no, calcula con BD.
        """
        tipo = (sub.tipo_item.data or '').strip().lower()
        item_id = int(sub.item_ref_id.data or 0)
        desc = (sub.descripcion_resumen.data or '').strip()
        total = sub.monto_total_original.data

        # Si ya vino desde el modal, lo usamos (evita divergencias)
        if desc and (total is not None):
            try:
                total_float = float(total)
                return tipo, item_id, desc, max(0.0, total_float)
            except Exception:
                pass

        # Fallback a cálculo local
        if tipo == 'pago':
            pg = db.session.get(Pago, item_id)
            if not pg:
                raise ValueError("Pago no encontrado para plan.")
            desc = desc or f"{pg.Pago_Tipo}"
            total = float(pg.Pago_Monto or 0.0)
            return tipo, item_id, desc, total

        elif tipo == 'articulo':
            art = db.session.get(Articulo, item_id)
            if not art:
                raise ValueError("Artículo no encontrado para plan.")
            desc = desc or f"{art.Articulo_Nombre}"
            total = float(art.Articulo_PrecioVenta or 0.0)
            return tipo, item_id, desc, total

        elif tipo == 'paquete':
            paq = paquetes_dict.get(item_id)
            if not paq:
                raise ValueError("Paquete no encontrado para plan.")
            # Usamos el precio final del paquete (con descuento del paquete)
            desc = desc or f"Paquete {paq['nombre']}"
            total = float(paq['total_final'] or 0.0)
            return tipo, item_id, desc, total

        raise ValueError("Tipo de ítem inválido para plan.")

    def _apply_policy_from_pago(plan: PlanCobro, pago_obj: Pago):
        """
        Traslada reglas básicas desde Pago hacia el plan (para evaluar al liquidar).
        Mantiene todo simple: descuento por vigencia (fecha), recargo % o fijo.
        """
        # Descuento: usamos Pago_Descuento_Porcentaje y Pago_Restricciones_Fecha
        plan.Aplica_Desc_Al_Liquidar = True if (pago_obj.Pago_Descuento_Porcentaje or pago_obj.Pago_Restricciones_Fecha) else False
        plan.Vigencia_Inicio = None  # simplificamos: solo fecha fin
        plan.Vigencia_Fin = pago_obj.Pago_Restricciones_Fecha
        plan.Porc_Descuento = (pago_obj.Pago_Descuento_Porcentaje or None)
        plan.Monto_Desc_Max = None

        # Recargo: % (día de mes/fecha exacta lo dejamos como política externa)
        if pago_obj.Pago_Tiene_Recargo:
            plan.Porc_Recargo = (pago_obj.Pago_Recargo_Porcentaje or None)
            if not plan.Porc_Recargo and pago_obj.Pago_Recargo_Porcentaje is None:
                plan.Porc_Recargo = None
        else:
            plan.Porc_Recargo = None
        plan.Monto_Rec_Fijo = None

    def _get_or_create_plan(est_id: int, sub) -> PlanCobro:
        """
        Busca plan abierto. Si no existe y 'crear_plan_si_no_existe' está activo,
        crea uno tomando snapshot de precio y reglas.
        """
        tipo = (sub.tipo_item.data or '').strip().lower()
        # Si se especificó plan_id, mejor cargarlo directo
        if sub.plan_id.data:
            pl = db.session.get(PlanCobro, int(sub.plan_id.data))
            if not pl:
                raise ValueError("Plan especificado no existe.")
            if pl.Estado != 'abierto':
                raise ValueError("El plan especificado no está abierto.")
            if pl.Est_ID != est_id:
                raise ValueError("El plan no pertenece al estudiante seleccionado.")
            return pl

        # Buscar plan abierto por combinación
        item_id = int(sub.item_ref_id.data or 0)
        pl = _find_open_plan(est_id, tipo, item_id)
        if pl:
            return pl

        # ¿Crear?
        if not sub.crear_plan_si_no_existe.data:
            raise ValueError("No existe plan y no se autorizó crear uno nuevo.")

        # Construir snapshot y crear plan
        tipo, item_id, desc, total = _build_plan_snapshot(est_id, sub)
        pl = PlanCobro(
            Est_ID=est_id,
            Articulo_ID=item_id if tipo == 'articulo' else None,
            Paquete_ID=item_id if tipo == 'paquete' else None,
            Pago_ID=item_id if tipo == 'pago' else None,
            Precio_Base_Snapshot=total,
            Descripcion_Resumen=desc,
            Monto_Total_Original=total,
            Saldo_Actual=total,
            Estado='abierto',
            Entregable=True if tipo in ('articulo', 'paquete') else False,
        )
        # Reglas si viene de Pago
        if tipo == 'pago':
            pg = db.session.get(Pago, item_id)
            if pg:
                _apply_policy_from_pago(pl, pg)

        db.session.add(pl)
        db.session.flush()
        return pl

    def _registrar_abono_y_posible_liquidacion(plan: PlanCobro,
                                               venta: Venta,
                                               metodo_norm: str,
                                               referencia_val: str | None,
                                               sub) -> tuple[float, float]:
        """
        Registra el abono (o liquidación) y, si el saldo llega a 0, calcula y adjunta
        el ajuste (descuento o recargo) a la misma venta como un Pago.
        Devuelve (descuento_aplicado, recargo_aplicado).
        """
        ahora = datetime.now()
        # Determinar monto del abono
        if sub.liquidar.data:
            abono_monto = float(plan.Saldo_Actual or 0.0)
        else:
            abono_monto = float(sub.monto_abono.data or 0.0)

        if abono_monto <= 0:
            raise ValueError("El monto del abono debe ser mayor a 0.")

        if abono_monto > float(plan.Saldo_Actual or 0.0):
            # Capar al saldo
            abono_monto = float(plan.Saldo_Actual or 0.0)

        # Crear Abono + enlazar a la venta
        ab = Abono(
            Plan_ID=plan.Plan_ID,
            Venta_ID=venta.Venta_ID,
            Monto_Abonado=abono_monto,
            Fecha_Abono=ahora,
            Metodo_Pago=metodo_norm,
            Referencia_Pago=(referencia_val if _requires_reference(metodo_norm) else None),
            Observaciones=(sub.observaciones.data or None)
        )
        db.session.add(ab)

        # Actualizar plan
        plan.Saldo_Actual = round(max(0.0, float(plan.Saldo_Actual or 0.0) - abono_monto), 2)
        plan.Fecha_Ultimo_Abono = ahora

        descuento_aplicado = 0.0
        recargo_aplicado = 0.0

        # ¿Llegó a 0? -> Liquidación y ajuste
        if float(plan.Saldo_Actual) <= 0.00001 and plan.Estado == 'abierto':
            # Determinar descuento/recargo según vigencia simple
            hoy = date.today()
            en_vigencia = True
            if plan.Vigencia_Inicio and plan.Vigencia_Fin:
                en_vigencia = (plan.Vigencia_Inicio <= hoy <= plan.Vigencia_Fin)
            elif plan.Vigencia_Fin and not plan.Vigencia_Inicio:
                en_vigencia = (hoy <= plan.Vigencia_Fin)
            elif plan.Vigencia_Inicio and not plan.Vigencia_Fin:
                en_vigencia = (hoy >= plan.Vigencia_Inicio)

            base = float(plan.Monto_Total_Original or 0.0)

            if plan.Aplica_Desc_Al_Liquidar and en_vigencia and plan.Porc_Descuento:
                descuento_aplicado = round(base * float(plan.Porc_Descuento) / 100.0, 2)
                if plan.Monto_Desc_Max:
                    descuento_aplicado = min(descuento_aplicado, float(plan.Monto_Desc_Max or 0.0))

                # Adjuntamos ajuste como Pago negativo
                pago_desc = Pago(
                    Pago_Tipo=f"Descuento liquidación plan #{plan.Plan_ID}",
                    Pago_Monto=-descuento_aplicado,
                    Pago_Fecha=hoy,
                    Est_ID=venta.Est_ID,
                    Pago_Es_Mensual=False
                )
                db.session.add(pago_desc)
                db.session.flush()
                venta.pagos.append(pago_desc)

            else:
                # ¿Recargo?
                rec_sobre_base = round(base * float(plan.Porc_Recargo or 0.0) / 100.0, 2) if plan.Porc_Recargo else 0.0
                rec_fijo = float(plan.Monto_Rec_Fijo or 0.0)
                recargo_aplicado = round(rec_sobre_base + rec_fijo, 2)

                if recargo_aplicado > 0:
                    pago_rec = Pago(
                        Pago_Tipo=f"Recargo liquidación plan #{plan.Plan_ID}",
                        Pago_Monto=recargo_aplicado,
                        Pago_Fecha=hoy,
                        Est_ID=venta.Est_ID,
                        Pago_Es_Mensual=False
                    )
                    db.session.add(pago_rec)
                    db.session.flush()
                    venta.pagos.append(pago_rec)

            # Crear registro de liquidación y cerrar plan
            liq = Liquidacion(
                Plan_ID=plan.Plan_ID,
                Venta_Final_ID=venta.Venta_ID,
                Fecha_Liquidacion=ahora,
                Descuento_Aplicado=descuento_aplicado,
                Recargo_Aplicado=recargo_aplicado,
                Base_Calculo='total_original',
                Nota_Reglas=None
            )
            db.session.add(liq)
            plan.Estado = 'liquidado'

            # NOTA: Si el plan es entregable (art/paq), aquí podrías disparar entrega/inventario.
            # Para mantener "mínima intrusión", lo dejamos como paso posterior.

        return descuento_aplicado, recargo_aplicado

    # ===========================
    #         POST
    # ===========================
    if request.method == 'POST':
        try:
            # Idempotencia (anti-duplicados por form_id)
            anti_dup_form_id = (request.form.get('form_id') or '').strip()
            processed_forms = session.get('processed_forms', [])
            if anti_dup_form_id and anti_dup_form_id in processed_forms:
                flash('Esta venta ya fue registrada (se evitó un duplicado).', 'info')
                return redirect(url_for("consulta_ventas"))

            # Ventas pendientes a consolidar
            raw_pend = (request.form.get('pendientes_ids') or '').strip()
            pendientes_ids = []
            if raw_pend:
                for tok in raw_pend.split(','):
                    tok = tok.strip()
                    if tok.isdigit():
                        pendientes_ids.append(int(tok))

            # Deducción de tipo de cliente
            tipo_cliente = (request.form.get("tipo_cliente") or "").strip().lower()
            est_id = form.estudiante_id.data if form.estudiante_id.data is not None else None
            ins_id = form.instructor_id.data if form.instructor_id.data is not None else None
            if not tipo_cliente:
                if est_id and int(est_id) != 0:
                    tipo_cliente = "estudiante"
                elif ins_id and int(ins_id) != 0:
                    tipo_cliente = "instructor"

            # Validación cliente
            if tipo_cliente == "estudiante":
                if est_id is None or int(est_id) == 0:
                    error_flags["cliente"] = True
                    flash("Debe seleccionar un estudiante.", "danger")
            elif tipo_cliente == "instructor":
                if ins_id is None or int(ins_id) == 0:
                    error_flags["cliente"] = True
                    flash("Debe seleccionar un instructor.", "danger")
            else:
                error_flags["cliente"] = True
                flash("Debe seleccionar un estudiante o un instructor.", "danger")

            # Método de pago + referencia
            metodo_crudo = (getattr(form, 'metodo_pago', None).data or "").strip() if hasattr(form, 'metodo_pago') else (request.form.get('metodo_pago','').strip())
            if not metodo_crudo:
                error_flags["metodo_pago"] = True
                flash("Debe seleccionar un método de pago.", "danger")
            metodo_norm = _normalize_method(metodo_crudo)

            referencia_val = (getattr(form, 'referencia_pago', None).data or "").strip() if hasattr(form, "referencia_pago") else (request.form.get('referencia_pago','').strip())
            if metodo_crudo and _requires_reference(metodo_norm) and not referencia_val:
                error_flags["referencia"] = True
                flash("La referencia es obligatoria para transferencia, tarjeta o depósito.", "danger")

            # === Parseo de Artículos ===
            items = []  # {'art_id','talla','qty','price':None|float}
            for k in request.form.keys():
                if k.startswith("articulos-") and "-id" in k:
                    art_key = request.form[k]  # "15:::M" o "15"
                    base_key = k.rsplit('-id', 1)[0]
                    qty_str = request.form.get(base_key + '-qty') or request.form.get(base_key + '-cantidad')
                    try:
                        qty = max(int(qty_str), 1) if qty_str is not None else 1
                    except Exception:
                        qty = 1

                    if KEY_SEP in art_key:
                        art_id_str, talla = art_key.split(KEY_SEP, 1)
                        art_id = int(art_id_str)
                        talla = talla or None
                    else:
                        art_id = int(art_key)
                        talla = None

                    items.append({'art_id': art_id, 'talla': talla, 'qty': qty, 'price': None})

            # === Parseo de Paquetes ===
            for k in request.form.keys():
                if k.startswith("paquetes-") and k.endswith("-id"):
                    base_key = k[:-3]  # quitar "-id"
                    try:
                        paq_id = int(request.form[k])
                    except Exception:
                        continue
                    qty_paq = request.form.get(base_key + 'qty') or request.form.get(base_key + 'cantidad') or "1"
                    try:
                        qty_paq = max(int(qty_paq), 1)
                    except Exception:
                        qty_paq = 1

                    paq = paquetes_dict.get(paq_id)
                    if not paq:
                        continue

                    for it in paq['items']:
                        total_qty = int(it['cantidad']) * qty_paq
                        price_override = float(it['precio_unit_ajustado'])
                        items.append({
                            'art_id': it['articulo_id'],
                            'talla': it['talla'],
                            'qty': total_qty,
                            'price': price_override
                        })

            # === Pagos seleccionados ===
            pagos_ids = []
            for k in request.form.keys():
                if k.startswith("pagos-") and k.endswith("-id"):
                    pagos_ids.append(request.form[k])

            # Reglas de contenido
            if tipo_cliente == "instructor":
                if len(items) == 0:
                    error_flags["articulos"] = True
                    flash("Para instructor, debe agregar al menos un artículo.", "danger")
                pagos_ids = []  # ignorar pagos para instructor
            elif tipo_cliente == "estudiante":
                if (len(items) == 0) and (len(pagos_ids) == 0) and (len(getattr(form, 'abonos', []).entries) == 0):
                    # Si no hay artículos, ni pagos, ni abonos → error
                    error_flags["articulos_o_pagos"] = True
                    flash("Para estudiante, agregue artículos y/o pagos, o registre un abono.", "danger")

            # Si hay errores: re-render
            if any(error_flags.values()):
                return render_template(
                    'registro_venta.html',
                    form=form,
                    articulos_dict=articulos_dict,
                    estudiantes=estudiantes,
                    instructores=instructores,
                    pagos_dict=pagos_dict,
                    paquetes_choices=paquetes_choices,
                    paquetes_dict=paquetes_dict,
                    KEY_SEP=KEY_SEP,
                    error_flags=error_flags,
                    tipo_cliente_seleccionado=tipo_cliente,
                    pendientes=pendientes,
                )

            # === Crear venta nueva
            nueva_venta = Venta(
                Est_ID=est_id if tipo_cliente == "estudiante" else None,
                Instructor_ID=ins_id if tipo_cliente == "instructor" else None,
                Metodo_Pago=metodo_norm,
                Referencia_Pago=referencia_val if _requires_reference(metodo_norm) else None,
                Fecha_Venta=datetime.now(),
            )

            # === Líneas + control de stock (solo artículos NO parciales)
            subtotal_lineas = 0.0
            for it in items:
                articulo = db.session.get(Articulo, it['art_id'])
                if not articulo:
                    error_flags["articulos"] = True
                    flash("Artículo no encontrado.", "danger")
                    return render_template(
                        'registro_venta.html',
                        form=form,
                        articulos_dict=articulos_dict,
                        estudiantes=estudiantes,
                        instructores=instructores,
                        pagos_dict=pagos_dict,
                        paquetes_choices=paquetes_choices,
                        paquetes_dict=paquetes_dict,
                        KEY_SEP=KEY_SEP,
                        error_flags=error_flags,
                        tipo_cliente_seleccionado=tipo_cliente,
                        pendientes=pendientes,
                    )

                precio = float(it['price']) if (it.get('price') is not None) else float(articulo.Articulo_PrecioVenta or 0)
                qty = int(it['qty'])
                talla = it['talla']

                tallas_obj = _safe_json_loads(articulo.Articulo_Tallas, default=None)

                if talla and isinstance(tallas_obj, dict):
                    existencia = int(tallas_obj.get(talla, 0))
                    if existencia < qty:
                        error_flags["articulos"] = True
                        flash(f"Stock insuficiente para {articulo.Articulo_Nombre} ({talla}). Disponible: {existencia}", "danger")
                        return render_template(
                            'registro_venta.html',
                            form=form,
                            articulos_dict=articulos_dict,
                            estudiantes=estudiantes,
                            instructores=instructores,
                            pagos_dict=pagos_dict,
                            paquetes_choices=paquetes_choices,
                            paquetes_dict=paquetes_dict,
                            KEY_SEP=KEY_SEP,
                            error_flags=error_flags,
                            tipo_cliente_seleccionado=tipo_cliente,
                            pendientes=pendientes,
                        )
                    tallas_obj[talla] = existencia - qty
                    articulo.Articulo_Tallas = json.dumps(tallas_obj)
                    articulo.Articulo_Existencia = sum(int(v) for v in tallas_obj.values())

                else:
                    existencia_total = int(getattr(articulo, "Articulo_Existencia", 0) or 0)
                    if existencia_total < qty:
                        error_flags["articulos"] = True
                        if talla:
                            flash(f"Stock insuficiente para {articulo.Articulo_Nombre} ({talla}). Disponible total: {existencia_total}", "danger")
                        else:
                            flash(f"Stock insuficiente para {articulo.Articulo_Nombre}. Disponible: {existencia_total}", "danger")
                        return render_template(
                            'registro_venta.html',
                            form=form,
                            articulos_dict=articulos_dict,
                            estudiantes=estudiantes,
                            instructores=instructores,
                            pagos_dict=pagos_dict,
                            paquetes_choices=paquetes_choices,
                            paquetes_dict=paquetes_dict,
                            KEY_SEP=KEY_SEP,
                            error_flags=error_flags,
                            tipo_cliente_seleccionado=tipo_cliente,
                            pendientes=pendientes,
                        )
                    articulo.Articulo_Existencia = max(0, existencia_total - qty)

                linea = VentaLinea(
                    venta=nueva_venta,
                    Articulo_ID=articulo.Articulo_ID,
                    Talla=talla,
                    Cantidad=qty,
                    Precio_Unitario=precio
                )
                db.session.add(linea)
                subtotal_lineas += precio * qty

            # === Asociar pagos (solo estudiante)
            total_conceptos = 0.0
            if tipo_cliente == "estudiante" and pagos_ids:
                for pid in pagos_ids:
                    try:
                        pid_int = int(pid)
                    except Exception:
                        continue
                    pago = db.session.get(Pago, pid_int)
                    if pago:
                        nueva_venta.pagos.append(pago)
                        total_conceptos += float(pago.Pago_Monto or 0.0)

            # Persistimos venta y obtenemos ID
            db.session.add(nueva_venta)
            db.session.flush()

            # === NUEVO: Procesar ABONOS (solo estudiante)
            if tipo_cliente != "estudiante" and len(getattr(form, 'abonos', []).entries) > 0:
                flash("Los abonos parciales aplican solo a estudiantes. Se ignoraron en esta venta.", "warning")

            ajustes_totales_desc = 0.0
            ajustes_totales_rec = 0.0

            if tipo_cliente == "estudiante":
                for sub in getattr(form, 'abonos', []).entries:
                    try:
                        plan = _get_or_create_plan(int(est_id), sub)
                        desc_apl, rec_apl = _registrar_abono_y_posible_liquidacion(
                            plan=plan,
                            venta=nueva_venta,
                            metodo_norm=metodo_norm,
                            referencia_val=referencia_val,
                            sub=sub
                        )
                        ajustes_totales_desc += float(desc_apl or 0.0)
                        ajustes_totales_rec  += float(rec_apl or 0.0)
                    except Exception as ex_ab:
                        error_flags["abonos"] = True
                        flash(f"❌ Error en abonos: {str(ex_ab)}", "danger")
                        # re-render conservando estado
                        return render_template(
                            'registro_venta.html',
                            form=form,
                            articulos_dict=articulos_dict,
                            estudiantes=estudiantes,
                            instructores=instructores,
                            pagos_dict=pagos_dict,
                            paquetes_choices=paquetes_choices,
                            paquetes_dict=paquetes_dict,
                            KEY_SEP=KEY_SEP,
                            error_flags=error_flags,
                            tipo_cliente_seleccionado=tipo_cliente,
                            pendientes=pendientes,
                        )

            # === Recalcular totales de pagos después de ajustes por liquidación
            total_conceptos = 0.0
            for p in nueva_venta.pagos:
                total_conceptos += float(p.Pago_Monto or 0.0)

            # === Registrar COBRO (si el modelo existe) con importes actualizados
            try:
                monto_cobrado = float(subtotal_lineas) + float(total_conceptos)
                if monto_cobrado > 0:
                    cobro = VentaCobro(
                        Venta_ID=nueva_venta.Venta_ID,
                        Metodo=metodo_norm,
                        Referencia=(referencia_val if _requires_reference(metodo_norm) else None),
                        Monto=monto_cobrado,
                        Fecha=datetime.now()
                    )
                    db.session.add(cobro)
            except NameError:
                pass

            # === ELIMINAR ventas PENDIENTES seleccionadas (reponiendo stock)
            if pendientes_ids:
                for vid in pendientes_ids:
                    vpend = (Venta.query
                             .filter(
                                 Venta.Venta_ID == vid,
                                 or_(Venta.Metodo_Pago.is_(None),
                                     Venta.Metodo_Pago == '',
                                     func.lower(Venta.Metodo_Pago).in_(['__pendiente__', 'pendiente']))
                             ).first())
                    if not vpend:
                        continue

                    lineas_pend = VentaLinea.query.filter(VentaLinea.Venta_ID == vid).all()
                    for lp in lineas_pend:
                        art = db.session.get(Articulo, lp.Articulo_ID)
                        if not art:
                            continue
                        qty = int(lp.Cantidad or 0)
                        tallas_obj = _safe_json_loads(art.Articulo_Tallas, default=None)

                        if lp.Talla and isinstance(tallas_obj, dict):
                            tallas_obj[lp.Talla] = int(tallas_obj.get(lp.Talla, 0)) + qty
                            art.Articulo_Tallas = json.dumps(tallas_obj)
                            art.Articulo_Existencia = sum(int(v) for v in tallas_obj.values())
                        else:
                            art.Articulo_Existencia = int(getattr(art, "Articulo_Existencia", 0) or 0) + qty

                    VentaLinea.query.filter(VentaLinea.Venta_ID == vid).delete(synchronize_session=False)
                    try:
                        db.session.execute(text("DELETE FROM venta_pago WHERE venta_id = :vid"), {"vid": vid})
                    except Exception:
                        pass
                    Venta.query.filter(Venta.Venta_ID == vid).delete(synchronize_session=False)

            # Commit final
            db.session.commit()

            # Marcar form_id como procesado
            if anti_dup_form_id:
                processed_forms.append(anti_dup_form_id)
                session['processed_forms'] = processed_forms[-50:]

            flash("Venta registrada con éxito.", "success")
            return redirect(url_for("consulta_ventas"))

        except IntegrityError:
            db.session.rollback()
            error_flags["db"] = True
            flash("❌ Error de integridad en la base de datos. Verifique los datos e intente de nuevo.", "danger")
            return render_template(
                'registro_venta.html',
                form=form,
                articulos_dict=articulos_dict,
                estudiantes=estudiantes,
                instructores=instructores,
                pagos_dict=pagos_dict,
                paquetes_choices=paquetes_choices,
                paquetes_dict=paquetes_dict,
                KEY_SEP=KEY_SEP,
                error_flags=error_flags,
                tipo_cliente_seleccionado=(request.form.get('tipo_cliente') or '').strip().lower(),
                pendientes=pendientes,
            )
        except Exception as e:
            db.session.rollback()
            error_flags["unexpected"] = True
            flash(f'❌ Error inesperado: {str(e)}', 'danger')
            return render_template(
                'registro_venta.html',
                form=form,
                articulos_dict=articulos_dict,
                estudiantes=estudiantes,
                instructores=instructores,
                pagos_dict=pagos_dict,
                paquetes_choices=paquetes_choices,
                paquetes_dict=paquetes_dict,
                KEY_SEP=KEY_SEP,
                error_flags=error_flags,
                tipo_cliente_seleccionado=(request.form.get('tipo_cliente') or '').strip().lower(),
                pendientes=pendientes,
            )

    # === GET o POST con errores ===
    return render_template(
        'registro_venta.html',
        form=form,
        articulos_dict=articulos_dict,
        estudiantes=estudiantes,
        instructores=instructores,
        pagos_dict=pagos_dict,
        paquetes_choices=paquetes_choices,
        paquetes_dict=paquetes_dict,
        KEY_SEP=KEY_SEP,
        error_flags=error_flags,
        tipo_cliente_seleccionado=request.form.get('tipo_cliente', '') if request.method == 'POST' else '',
        pendientes=pendientes,
    )


# -----------------------------
# Consulta ventas
# -----------------------------
@app.route('/consulta/ventas', methods=['GET'])
def consulta_ventas():
    # ===== Filtros =====
    inicio_str = (request.args.get('inicio') or '').strip()
    fin_str    = (request.args.get('fin') or '').strip()
    metodo     = _normalize_method((request.args.get('metodo') or '').strip())
    tipo       = (request.args.get('tipo') or 'todos').strip().lower()
    q          = (request.args.get('q') or '').strip()
    estado     = (request.args.get('estado') or 'todas').strip().lower()  # NUEVO

    # Si alguien elige "método = pendiente", lo tratamos como estado=pendientes
    if metodo in ('pendiente', '__pendiente__', '__pendiente', '__PENDIENTE__'):
        estado = 'pendientes'
        metodo = ''

    # Si el estado es "pendientes", NO aplicar filtro por método (pendiente no tiene método)
    if estado == 'pendientes':
        metodo = ''

    # ===== Fechas =====
    dt_ini = None
    dt_fin = None
    if inicio_str:
        try:
            dt_ini = datetime.strptime(inicio_str, '%Y-%m-%d')
        except Exception:
            dt_ini = None
    if fin_str:
        try:
            dt_fin = datetime.strptime(fin_str, '%Y-%m-%d') + timedelta(days=1)
        except Exception:
            dt_fin = None

    # ===== Query base =====
    query = (Venta.query
             .options(
                 joinedload(Venta.estudiante),
                 joinedload(Venta.instructor),
                 selectinload(Venta.lineas),
                 selectinload(Venta.pagos)
             )
             .order_by(Venta.Fecha_Venta.desc()))

    # Tipo de cliente
    if tipo == 'estudiante':
        query = query.filter(Venta.Est_ID.isnot(None))
    elif tipo == 'instructor':
        query = query.filter(Venta.Instructor_ID.isnot(None))

    # Método (solo si quedó algo después de la lógica de pendientes)
    if metodo:
        query = query.filter(Venta.Metodo_Pago == metodo)

    # Rango de fechas
    if dt_ini:
        query = query.filter(Venta.Fecha_Venta >= dt_ini)
    if dt_fin:
        query = query.filter(Venta.Fecha_Venta < dt_fin)

    # ===== Filtro de ESTADO =====
    # Consideramos PENDIENTE cuando:
    #  - Cobro_Pendiente == True (si existe esa columna)
    #  - o Metodo_Pago es '__PENDIENTE__' / 'pendiente' (en cualquier casing)
    #  - o Metodo_Pago es NULL o '' (cadena vacía)
    base_pend_expr = or_(
        func.lower(Venta.Metodo_Pago) == '__pendiente__',
        func.lower(Venta.Metodo_Pago) == 'pendiente',
        Venta.Metodo_Pago.is_(None),
        Venta.Metodo_Pago == ''
    )
    if hasattr(Venta, 'Cobro_Pendiente'):
        pendiente_expr = or_(Venta.Cobro_Pendiente == True, base_pend_expr)
    else:
        pendiente_expr = base_pend_expr

    if estado == 'pendientes':
        query = query.filter(pendiente_expr)
    elif estado == 'cobradas':
        query = query.filter(~pendiente_expr)

    # Búsqueda libre
    if q:
        patron = f"%{q}%"
        query = (query
                 .outerjoin(Estudiante, Estudiante.Est_ID == Venta.Est_ID)
                 .outerjoin(Instructor, Instructor.Instructor_ID == Venta.Instructor_ID)
                 .filter(or_(
                     Estudiante.Est_Nombre.ilike(patron),
                     Estudiante.Est_ApellidoP.ilike(patron),
                     Instructor.Instructor_Nombre.ilike(patron),
                     Instructor.Instructor_ApellidoP.ilike(patron),
                     Venta.Metodo_Pago.ilike(patron),
                     Venta.Referencia_Pago.ilike(patron)
                 ))
                 .distinct())

    # === Ejecutar y armar reporte (mantiene totales correctos) ===
    ventas_raw = query.all()
    ventas, kpis = _armar_reporte(ventas_raw)

    # === Link historial estudiante (igual que ya tenías) ===
    est_por_venta = {}
    for _v in ventas_raw:
        vid = getattr(_v, 'Venta_ID', None) or getattr(_v, 'id', None)
        est_id = (getattr(_v, 'Est_ID', None) or getattr(_v, 'estudiante_id', None))
        if not est_id and hasattr(_v, 'estudiante') and _v.estudiante:
            est_id = (getattr(_v.estudiante, 'Est_ID', None) or getattr(_v.estudiante, 'id', None))
        if vid and est_id:
            est_por_venta[vid] = est_id

    for row in ventas:
        vid = row.get('id')
        est_id = est_por_venta.get(vid)
        if row.get('cliente_tipo') == 'estudiante' and est_id:
            row['cliente_url'] = url_for('historial_ventas_estudiante', estudiante_id=est_id)
        else:
            row['cliente_url'] = None

    return render_template(
        'consulta_ventas.html',
        ventas=ventas,
        kpis=kpis,
        filtros={
            'inicio': inicio_str,
            'fin': fin_str,
            'metodo': metodo,
            'tipo': tipo,
            'q': q,
            'estado': estado
        }
    )


#
# Eliminar venta
#
@app.route('/ventas/<int:venta_id>/eliminar', methods=['POST'])
def eliminar_venta(venta_id):
    import json
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy import text
    from sqlalchemy.orm import selectinload

    # Cargar venta con sus líneas
    v = (Venta.query
         .options(selectinload(Venta.lineas))
         .get_or_404(venta_id))

    try:
        # === Restablecer inventario por cada línea ===
        for ln in (v.lineas or []):
            art = db.session.get(Articulo, ln.Articulo_ID)
            if not art:
                continue

            qty = int(getattr(ln, 'Cantidad', 0) or 0)
            talla = getattr(ln, 'Talla', None)

            # Intentar parsear JSON de tallas
            tallas_obj = None
            try:
                tallas_obj = json.loads(art.Articulo_Tallas) if art.Articulo_Tallas else None
            except Exception:
                tallas_obj = None

            if talla and isinstance(tallas_obj, dict):
                key = str(talla)
                tallas_obj[key] = int(tallas_obj.get(key, 0) or 0) + qty
                art.Articulo_Tallas = json.dumps(tallas_obj)

                # Recalcular existencia total a partir del dict
                try:
                    art.Articulo_Existencia = sum(int(vv or 0) for vv in tallas_obj.values())
                except Exception:
                    # Fallback por si algún valor vino raro
                    art.Articulo_Existencia = int(getattr(art, 'Articulo_Existencia', 0) or 0) + qty
            else:
                # Sin talla o sin dict válido de tallas → sumar a existencia total
                exist_total = int(getattr(art, 'Articulo_Existencia', 0) or 0)
                art.Articulo_Existencia = exist_total + qty

        # === Limpiar dependencias (por si no hay cascade) ===
        # Cobros (si el modelo existe)
        try:
            VentaCobro.query.filter_by(Venta_ID=v.Venta_ID).delete(synchronize_session=False)
        except NameError:
            pass  # Si no tienes el modelo, lo ignoramos con seguridad

        # Relación venta_pago (tabla puente)
        try:
            db.session.execute(text("DELETE FROM venta_pago WHERE venta_id = :vid"), {"vid": v.Venta_ID})
        except Exception:
            pass

        # Líneas (si tu relación no tiene cascade delete)
        try:
            VentaLinea.query.filter(VentaLinea.Venta_ID == v.Venta_ID).delete(synchronize_session=False)
        except Exception:
            pass

        # Eliminar la venta
        db.session.delete(v)
        db.session.commit()

        flash(f'La venta #{venta_id} fue eliminada y el inventario fue restablecido.', 'success')

    except IntegrityError:
        db.session.rollback()
        flash('No se pudo eliminar la venta por un problema de integridad.', 'danger')
    except Exception as e:
        db.session.rollback()
        flash(f'Ocurrió un error al eliminar la venta: {e}', 'danger')

    next_url = request.form.get('next') or url_for('consulta_ventas')
    return redirect(next_url)


#
# Detalle de la venta
#
@app.get('/ventas/<int:venta_id>')
def venta_detalle(venta_id: int):
    v = (Venta.query
         .options(
             joinedload(Venta.estudiante),
             joinedload(Venta.instructor),
             selectinload(Venta.lineas),
             selectinload(Venta.pagos)
         )
         .filter(Venta.Venta_ID == venta_id)
         .first())
    if not v:
        flash(f"Venta #{venta_id} no encontrada.", "warning")
        return redirect(url_for('consulta_ventas'))

    # Reusar ensamblado existente
    ventas, _kpis = _armar_reporte([v])
    det = ventas[0] if ventas else None
    if not det:
        flash(f"No fue posible armar el detalle de la venta #{venta_id}.", "danger")
        return redirect(url_for('consulta_ventas'))

    # <- NUEVO: si piden parcial, devolvemos solo el fragmento embebible
    if request.args.get('partial') == '1':
        return render_template(
            'venta_detalle_partial.html',
            v=det,
            venta_id=venta_id,
            rid=request.args.get('rid', ''),  # id del acordeón en la tabla
            back=(request.args.get('next') or url_for('consulta_ventas'))
        )

    # Página completa (si algún día quieres abrir en pestaña)
    return render_template(
        'venta_detalle.html',
        v=det,
        venta_id=venta_id,
        back=(request.args.get('next') or url_for('consulta_ventas'))
    )

#
#  recibo
#
@app.get('/ventas/<int:venta_id>/recibo')
def venta_recibo(venta_id: int):
    v = (Venta.query
         .options(
             joinedload(Venta.estudiante),
             joinedload(Venta.instructor),
             selectinload(Venta.lineas),
             selectinload(Venta.pagos)
         )
         .filter(Venta.Venta_ID == venta_id)
         .first())
    if not v:
        flash(f"Venta #{venta_id} no encontrada.", "warning")
        return redirect(url_for('consulta_ventas'))

    ventas, _kpis = _armar_reporte([v])
    det = ventas[0] if ventas else None
    if not det:
        flash(f"No fue posible armar el detalle de la venta #{venta_id}.", "danger")
        return redirect(url_for('consulta_ventas'))

    back = request.args.get('next') or url_for('venta_detalle', venta_id=venta_id)
    return render_template('venta_recibo.html', v=det, venta_id=venta_id, back=back)

# ----
# Historial estudiante
#
@app.route('/estudiantes/<int:estudiante_id>/historial-ventas', endpoint='historial_ventas_estudiante')
def historial_ventas_estudiante(estudiante_id):
    # --- 1) Estudiante ---
    est = Estudiante.query.get_or_404(estudiante_id)

    # Helpers
    def _first_attr(o, names, default=None):
        for n in names:
            if hasattr(o, n):
                v = getattr(o, n)
                if v is not None:
                    return v
        return default

    # Encabezado
    fecha_ingreso = _first_attr(est, [
        'Est_FechaIngreso', 'fecha_ingreso', 'Est_FechaRegistro',
        'Est_FechaAlta', 'created_at', 'fecha_registro'
    ])
    est_status_raw = _first_attr(est, ['status','estatus','Est_Status','Est_Estatus','Est_Activo'])
    est_status = ('Activo' if est_status_raw is True else
                  'Inactivo' if est_status_raw is False else
                  (est_status_raw or '—'))

    # --- 2) Traer ventas del estudiante (RAW) ---
    ventas_raw = (Venta.query
                  .filter(Venta.Est_ID == estudiante_id)
                  .options(
                      joinedload(Venta.estudiante),
                      joinedload(Venta.instructor),
                      selectinload(Venta.lineas),
                      selectinload(Venta.pagos)
                  )
                  .order_by(Venta.Fecha_Venta.desc())
                  .all())

    # --- 3) Estandarizar con _armar_reporte para que coincida con "detalle de venta" ---
    ventas_armadas, _kpis = _armar_reporte(ventas_raw)
    # ventas_armadas: cada elemento trae:
    #  id, fecha, metodo, referencia, items (articulo, talla, cantidad, precio_unit, total_linea),
    #  subtotal_items, pagos (tipo, cond, vence, monto_bruto, pct_aplicado, descuento_monto, monto_neto),
    #  descuento_pagos, total_venta, ...

    # --- 4) Agrupar por día usando las ventas armadas ---
    from collections import OrderedDict
    grupos = OrderedDict()

    for det in ventas_armadas:
        fecha = det.get('fecha')
        try:
            fecha_key = fecha.date() if fecha else None
        except Exception:
            fecha_key = None

        # Armar registro compatible con el template
        total_art = float(det.get('subtotal_items', 0.0) or 0.0)
        pagos_det = list(det.get('pagos', []) or [])
        total_pag = float(sum(float(p.get('monto_neto', 0.0) or 0.0) for p in pagos_det))
        total_ven = float(det.get('total_venta', total_art + total_pag) or (total_art + total_pag))

        grupos.setdefault(fecha_key, []).append({
            'venta': {'Venta_ID': det.get('id'), 'status': det.get('status')},
            'fecha_venta': fecha,
            'metodo_pago': det.get('metodo'),
            'referencia': det.get('referencia'),
            'articulos': list(det.get('items', []) or []),
            'pagos': pagos_det,
            'total_articulos': total_art,
            'total_pagos': total_pag,
            'total_venta': total_ven,
        })

    # Orden dentro del día (más recientes primero)
    for k in list(grupos.keys()):
        grupos[k].sort(key=lambda x: x['fecha_venta'] or 0, reverse=True)

    total_global = float(sum(h['total_venta'] for lst in grupos.values() for h in lst))

    # Para KPI "Total de ventas" el template usa {{ ventas|length }}
    ventas_para_kpi = ventas_armadas

    return render_template(
        'historial_ventas_estudiante.html',
        est=est,
        fecha_ingreso=fecha_ingreso,
        est_status=est_status,
        grupos=grupos,
        total_global=total_global,
        ventas=ventas_para_kpi  # <- cuenta coincide con detalle
    )

#
#
#
@app.get('/ventas/<int:venta_id>', endpoint='detalle_venta')
def detalle_venta_view(venta_id: int):
    v = (Venta.query
         .options(
             joinedload(Venta.estudiante),
             joinedload(Venta.instructor),
             selectinload(Venta.lineas),
             selectinload(Venta.pagos)
         )
         .filter(Venta.Venta_ID == venta_id)
         .first())
    if not v:
        flash(f"Venta #{venta_id} no encontrada.", "warning")
        return redirect(url_for('consulta_ventas'))

    # Ensamblado existente
    ventas, _kpis = _armar_reporte([v])
    det = ventas[0] if ventas else None
    if not det:
        flash(f"No fue posible armar el detalle de la venta #{venta_id}.", "danger")
        return redirect(url_for('consulta_ventas'))

    # === OBTENER ESTUDIANTE_ID DE MANERA ROBUSTA ===
    estudiante_id = None
    
    # Primero intentar desde la relación estudiante
    if hasattr(v, 'estudiante') and v.estudiante:
        estudiante_id = (
            getattr(v.estudiante, 'Est_ID', None) or
            getattr(v.estudiante, 'id', None) or
            getattr(v.estudiante, 'Estudiante_ID', None)
        )
    
    # Si no se encontró, intentar desde el atributo directo
    if not estudiante_id:
        estudiante_id = getattr(v, 'Est_ID', None) or getattr(v, 'estudiante_id', None)
    
    # Si usas render parcial embebido
    if request.args.get('partial') == '1':
        return render_template(
            'venta_detalle_partial.html',
            v=det,
            venta_id=venta_id,
            rid=request.args.get('rid', ''),
            back=(request.args.get('next') or url_for('consulta_ventas')),
            estudiante_id=estudiante_id  # <-- importante
        )

    # Página completa
    return render_template(
        'venta_detalle.html',
        v=det,
        venta_id=venta_id,
        back=(request.args.get('next') or url_for('consulta_ventas')),
        estudiante_id=estudiante_id  # <-- importante
    )


#
# Ventas pendientes
#
@app.route('/api/ventas/pendientes', methods=['GET'])
def api_ventas_pendientes():
    tipo = (request.args.get('tipo') or '').strip().lower()  # 'estudiante' | 'instructor'
    cliente_id = (request.args.get('cliente_id') or '').strip()
    if not tipo or not cliente_id:
        return jsonify({'ventas': []})

    q = (Venta.query
         .options(
             joinedload(Venta.estudiante),
             joinedload(Venta.instructor),
             selectinload(Venta.lineas),
             selectinload(Venta.pagos))
         .filter(_pendiente_expr())
         .order_by(Venta.Fecha_Venta.desc()))

    if tipo == 'estudiante':
        q = q.filter(Venta.Est_ID == int(cliente_id))
    elif tipo == 'instructor':
        q = q.filter(Venta.Instructor_ID == int(cliente_id))
    else:
        return jsonify({'ventas': []})

    out = []
    for v in q.all():
        vid = getattr(v, 'Venta_ID', None) or getattr(v, 'id', None)
        cliente_nombre = (
            f"{v.estudiante.Est_Nombre} {v.estudiante.Est_ApellidoP}" if v.estudiante else
            f"{v.instructor.Instructor_Nombre} {v.instructor.Instructor_ApellidoP}" if v.instructor else '—'
        )
        lineas = []
        for l in (v.lineas or []):
            # ajusta prioridad de IDs según tu esquema:
            var_id = (getattr(l, 'Variante_ID', None) or
                      getattr(l, 'ArticuloVariante_ID', None) or
                      getattr(l, 'variante_id', None) or
                      getattr(l, 'articulo_variante_id', None) or
                      getattr(l, 'Articulo_ID', None))
            cant = int(getattr(l, 'Cantidad', None) or getattr(l, 'cantidad', None) or 1)
            if var_id:
                lineas.append({'id': str(var_id), 'cantidad': cant})

        pagos = []
        for p in (v.pagos or []):
            pago_id = (getattr(p, 'Pago_ID', None) or
                       getattr(p, 'Promocion_ID', None) or
                       getattr(p, 'pago_id', None) or
                       getattr(p, 'promocion_id', None) or
                       getattr(p, 'id', None))
            cant = int(getattr(p, 'Cantidad', None) or getattr(p, 'cantidad', None) or 1)
            if pago_id:
                pagos.append({'id': str(pago_id), 'cantidad': cant})

        out.append({
            'id': vid,
            'fecha': v.Fecha_Venta.strftime('%Y-%m-%d %H:%M') if v.Fecha_Venta else '',
            'cliente_nombre': cliente_nombre,
            'tipo_cliente': tipo,
            'cliente_id': int(cliente_id),
            'lineas': lineas,
            'pagos': pagos,
        })

    return jsonify({'ventas': out})

################################################
## Registro abonos
@app.route('/registro/abonos', methods=['GET', 'POST'])
def registro_abonos():
    # ===== Imports locales =====
    from decimal import Decimal
    from datetime import datetime, date, timedelta
    import traceback, hashlib, json

    from flask import current_app, request, session, flash, redirect, url_for, render_template
    from sqlalchemy import or_, func
    from sqlalchemy.orm import joinedload, selectinload
    from sqlalchemy.exc import IntegrityError

    # Extensiones / Modelos / Forms
    try:
        from extensions import db
    except Exception as ie:
        current_app.logger.error("No se pudo importar db desde extensions: %s", ie)
        raise
    from models import Venta, Pago, PlanCobro, Estudiante, Instructor
    try:
        from models import VentaCobro
    except Exception:
        VentaCobro = None
    # 👇 Importa el form aquí para evitar NameError si el orden de importación arriba cambia
    from forms import VentaForm

    # Helpers
    from billing_utils import (
        money, normalize_method, requires_reference, parse_conditions, compute_full_net
    )
    from plan_utils import (
        get_or_create_plan, find_open_plan, registrar_abono, liquidar_plan
    )

    # ===== Helpers utilitarios (locales, solo formateo/compatibilidad) =====
    def _to_float(x, default=0.0) -> float:
        if x is None:
            return float(default)
        try:
            if isinstance(x, Decimal):
                return float(x)
            return float(x)
        except Exception:
            return float(default)

    def _iso_or_none(d):
        """Permite Date o DateTime; regresa ISO8601 o None."""
        if not d:
            return None
        try:
            if isinstance(d, datetime):
                return d.date().isoformat()
            if isinstance(d, date):
                return d.isoformat()
        except Exception:
            pass
        return None

    # 🧱 Helper DEDUP: key determinística
    def _make_idempotency_key(est_id, metodo_norm, referencia_val, cobro_pendiente, pagos_items, planes_movs):
        """
        Crea un hash reproducible del payload funcional para detectar reintentos.
        Ordena dicts y listas para que sea estable.
        """
        payload = {
            "est_id": int(est_id) if est_id else None,
            "metodo_norm": (metodo_norm or "").lower(),
            "referencia": (referencia_val or "").strip() if referencia_val else None,
            "pendiente": bool(cobro_pendiente),
            # Normalizamos la lista de items para que el orden no importe
            "pagos": sorted([
                {
                    "pago_id": int(i["pago_id"]),
                    "qty": int(i["qty"]),
                    "unit": float(i["unit"]),
                    "subtotal_catalogo": float(i["subtotal_catalogo"]),
                    "charge_now": float(i["charge_now"]),
                    "is_partial": bool(i["is_partial"]),
                    # full_breakdown afecta el neto; guardamos solo el neto para el hash
                    "neto_full": float(i["full_breakdown"].get("neto", 0.0))
                }
                for i in pagos_items
            ], key=lambda x: (x["pago_id"], x["qty"], x["charge_now"], x["is_partial"], x["neto_full"])),
            "planes": sorted([
                {
                    "plan_id": int(m["plan_id"]),
                    "accion": (m["accion"] or "").lower(),
                    "monto": float(m["monto"])
                }
                for m in planes_movs
            ], key=lambda x: (x["plan_id"], x["accion"], x["monto"])),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ====== Form ======
    form = VentaForm()
    if request.method == 'POST':
        form.process(request.form)

    # === Catálogos (cliente) ===
    estudiantes = (Estudiante.query
                   .order_by(Estudiante.Est_Nombre.asc(), Estudiante.Est_ApellidoP.asc())
                   .all())
    instructores = (Instructor.query
                    .order_by(Instructor.Instructor_Nombre.asc(), Instructor.Instructor_ApellidoP.asc())
                    .all())

    # === Choices básicos ===
    form.estudiante_id.choices = [(0, "— Selecciona —")] + [
        (e.Est_ID, f"{e.Est_Nombre} {e.Est_ApellidoP}") for e in estudiantes
    ]
    form.instructor_id.choices = [(0, "— Selecciona —")] + [
        (i.Instructor_ID, f"{i.Instructor_Nombre} {i.Instructor_ApellidoP}") for i in instructores
    ]

    # === Pagos (conceptos) ===
    pagos = Pago.query.order_by(Pago.Pago_Tipo.asc()).all()

    def _getattr_safe(o, name, default=None):
        try:
            return getattr(o, name)
        except Exception:
            return default

    # ===== Paso 3: metadata de Pagos =====
    pagos_dict = {}
    for p in pagos:
        try:
            condiciones = parse_conditions(getattr(p, 'Pago_Condiciones', None))
        except Exception:
            condiciones = []

        pagos_dict[p.Pago_ID] = {
            "nombre": p.Pago_Tipo,
            "monto": _to_float(p.Pago_Monto, 0.0),
            "descuento_tipo": (getattr(p, 'Pago_Descuento_Tipo', '') or '').strip().lower(),
            "descuento_porcentaje": _to_float(getattr(p, 'Pago_Descuento_Porcentaje', 0.0), 0.0),
            "condiciones": condiciones,
            "valido_hasta": _iso_or_none(getattr(p, 'Pago_Restricciones_Fecha', None)),
            "recargo_porcentaje": _to_float(_getattr_safe(p, 'Pago_Recargo_Porcentaje', 0.0), 0.0),
            "recargo_dia_corte": int(_getattr_safe(p, 'Pago_Recargo_DiaCorte', 0) or 0),
        }

    if hasattr(form, 'pagos'):
        form.pagos.choices = [
            (p.Pago_ID, f"{p.Pago_Tipo} - ${_to_float(p.Pago_Monto, 0):.2f}")
            for p in pagos
        ]

    # === Pendientes (ventas sin cobro aplicado) ===
    pend_rows = (Venta.query
                 .options(
                     joinedload(Venta.estudiante),
                     joinedload(Venta.instructor),
                     selectinload(Venta.pagos)
                 )
                 .filter(
                     or_(
                         Venta.Metodo_Pago.is_(None),
                         Venta.Metodo_Pago == '',
                         func.lower(Venta.Metodo_Pago) == '__pendiente__',
                         func.lower(Venta.Metodo_Pago) == 'pendiente'
                     )
                 )
                 .order_by(Venta.Fecha_Venta.desc())
                 .all())
    pendientes = []
    for v in pend_rows:
        if getattr(v, 'Est_ID', None):
            cliente_tipo = 'estudiante'
            cliente_id = v.Est_ID
            cliente_nombre = (f"{v.estudiante.Est_Nombre} {v.estudiante.Est_ApellidoP}"
                              if getattr(v, 'estudiante', None) else '—')
        elif getattr(v, 'Instructor_ID', None):
            cliente_tipo = 'instructor'
            cliente_id = v.Instructor_ID
            cliente_nombre = (f"{v.instructor.Instructor_Nombre} {v.instructor.Instructor_ApellidoP}"
                              if getattr(v, 'instructor', None) else '—')
        else:
            cliente_tipo = 'desconocido'
            cliente_id = None
            cliente_nombre = '—'

        pagos_list = []
        for p in getattr(v, 'pagos', []):
            pagos_list.append({
                "Pago_ID": p.Pago_ID,
                "Descripcion": p.Pago_Tipo,
                "Monto": _to_float(p.Pago_Monto, 0.0)
            })

        pendientes.append({
            "Venta_ID": v.Venta_ID,
            "Fecha_Venta": v.Fecha_Venta.isoformat() if v.Fecha_Venta else None,
            "cliente_tipo": cliente_tipo,
            "cliente_id": cliente_id,
            "cliente_nombre": cliente_nombre,
            "pagos": pagos_list,
            "Metodo_Pago": v.Metodo_Pago
        })

    # === Banderas UI ===
    error_flags = {
        "cliente": False,
        "pagos": False,
        "abonos": False,
        "metodo_pago": False,
        "referencia": False,
        "db": False,
        "unexpected": False,
    }

    if request.method == 'POST':
        try:
            today = datetime.now().date()

            # Anti-duplicados básico (si tu HTML ya manda form_id)
            anti_dup_form_id = (request.form.get('form_id') or '').strip()
            processed_forms = session.get('processed_forms', [])

            # Cliente (solo estudiante)
            est_id = form.estudiante_id.data if form.estudiante_id.data is not None else None
            if est_id is None or int(est_id) == 0:
                error_flags["cliente"] = True
                flash("Debe seleccionar un estudiante.", "danger")

            # Cobro pendiente (PASO 9)
            cobro_pendiente = (request.form.get('cobro_pendiente') is not None)

            # Método de pago + referencia (si NO es pendiente)
            metodo_crudo = (getattr(form, 'metodo_pago', None).data or "").strip() if hasattr(form, 'metodo_pago') else (request.form.get('metodo_pago','').strip())
            metodo_norm = normalize_method(metodo_crudo)
            referencia_val = (getattr(form, 'referencia_pago', None).data or "").strip() if hasattr(form, "referencia_pago") else (request.form.get('referencia_pago','').strip())

            if not cobro_pendiente:
                if not metodo_crudo:
                    error_flags["metodo_pago"] = True
                    flash("Debe seleccionar un método de pago.", "danger")
                if metodo_crudo and requires_reference(metodo_norm) and not referencia_val:
                    error_flags["referencia"] = True
                    flash("La referencia es obligatoria para transferencia, tarjeta o depósito.", "danger")
            else:
                # PASO 9: normalización para pendiente
                metodo_norm = "__pendiente__"
                referencia_val = None

            # === Parseo de pagos seleccionados (+ cantidades y parciales) ===
            pagos_items = []
            idx = 0
            EPS = 1e-6  # NUEVO: tolerancia para comparaciones flotantes
            while True:
                pid = request.form.get(f'pagos-{idx}-id', None)
                if pid is None:
                    break
                try:
                    pid_int = int(pid)
                except Exception:
                    idx += 1
                    continue

                qty_raw = request.form.get(f'pagos-{idx}-qty', "1")
                try:
                    qty = max(int(qty_raw), 1)
                except Exception:
                    qty = 1

                monto_parcial_raw = request.form.get(f'pagos-{idx}-monto-parcial', None)
                is_full_flag = request.form.get(f'pagos-{idx}-full', None) is not None

                meta = pagos_dict.get(pid_int, {})
                unit = float(meta.get("monto", 0.0))
                subtotal = unit * qty
                subtotal = money(subtotal)  # NUEVO: redondeo contable

                # --- Cálculo de NETO FULL (subtotal - descuento + recargo) ---
                full_calc = compute_full_net(
                    unit_price=unit,
                    qty=qty,
                    discount_pct=meta.get("descuento_porcentaje", 0.0),
                    discount_methods=meta.get("condiciones", []),
                    discount_valid_until=meta.get("valido_hasta"),
                    surcharge_pct=meta.get("recargo_porcentaje", 0.0),
                    surcharge_day_cut=meta.get("recargo_dia_corte", 0),
                    method_norm=metodo_norm,
                    today=today,
                    surcharge_on="post_discount",
                )
                neto_full = money(full_calc["neto"])  # NUEVO

                # --- PASO 6 + PASO 9: Reglas parcial vs full + pendiente ---
                is_partial = False
                charge_now = 0.0

                if cobro_pendiente:
                    # PASO 9: no se cobra nada hoy
                    is_partial = False
                    charge_now = 0.0
                else:
                    if (monto_parcial_raw is not None) and not is_full_flag:
                        try:
                            monto_parcial = float(monto_parcial_raw or 0)
                        except Exception:
                            monto_parcial = 0.0
                        monto_parcial = money(monto_parcial)  # NUEVO

                        if (monto_parcial + EPS) >= subtotal:
                            # Parcial >= subtotal ⇒ FULL/NETO
                            is_partial = False
                            charge_now = neto_full
                            flash("El monto parcial ingresado fue ≥ al subtotal del concepto: se procesó como pago completo con descuento/recargo aplicable.", "info")
                        elif monto_parcial > 0:
                            is_partial = True
                            charge_now = max(0.0, min(subtotal, monto_parcial))
                        else:
                            is_partial = False
                            charge_now = neto_full
                    else:
                        is_partial = False
                        charge_now = neto_full

                charge_now = money(charge_now)  # NUEVO: redondeo final del renglón

                # NUEVO: Bloqueo de “céntimos fantasma” (0.01) por redondeos de UI
                if 0 < charge_now < 0.02:
                    charge_now = 0.0
                    flash("El monto calculado resultó demasiado pequeño y se redondeó a $0.00 para evitar errores por centavos.", "warning")

                pagos_items.append({
                    "pago_id": pid_int,
                    "qty": qty,
                    "unit": unit,
                    "subtotal_catalogo": money(subtotal),
                    "charge_now": charge_now,
                    "is_partial": bool(is_partial),
                    "full_breakdown": {**full_calc, "neto": neto_full}  # NUEVO: neto ya redondeado
                })
                idx += 1

            # === Movimientos de planes (PASO 9: monto a cero si pendiente) ===
            planes_movs = []
            idx = 0
            while True:
                pid = request.form.get(f'planes-{idx}-id', None)
                if pid is None:
                    break
                accion = (request.form.get(f'planes-{idx}-accion', '') or '').strip().lower()
                monto_raw = request.form.get(f'planes-{idx}-monto', '0')
                try:
                    monto_val = max(float(monto_raw or 0), 0.0)
                except Exception:
                    monto_val = 0.0
                monto_val = money(monto_val)  # NUEVO
                planes_movs.append({
                    "plan_id": int(pid),
                    "accion": accion,
                    "monto": (0.0 if cobro_pendiente else monto_val)  # PASO 9 + NUEVO redondeo
                })
                idx += 1

            if (len(pagos_items) == 0) and (len(planes_movs) == 0):
                error_flags["pagos"] = True
                error_flags["abonos"] = True
                flash("Debe agregar al menos un pago o un movimiento de plan.", "danger")

            if any([error_flags["cliente"], error_flags["pagos"], error_flags["abonos"],
                    error_flags["metodo_pago"], error_flags["referencia"]]):
                return render_template(
                    'registro_abonos.html',
                    form=form,
                    estudiantes=estudiantes,
                    instructores=instructores,
                    pagos_dict=pagos_dict,
                    error_flags=error_flags,
                    tipo_cliente_seleccionado="estudiante",
                    pendientes=pendientes,
                    planes_abiertos=[],
                )

            # 🧱 DEDUP 1: form_id explícito desde el HTML
            if anti_dup_form_id and anti_dup_form_id in processed_forms:
                flash('Este registro ya fue procesado (se evitó un duplicado).', 'info')
                return redirect(url_for("consulta_ventas"))

            # 🧱 DEDUP 2: idempotency key determinística (fallback si no hay form_id)
            idem_key = _make_idempotency_key(est_id, metodo_norm, referencia_val, cobro_pendiente, pagos_items, planes_movs)
            processed_keys = session.get('processed_idem_keys', [])
            if idem_key in processed_keys:
                flash('Este registro es idéntico a uno ya confirmado (se evitó un duplicado).', 'info')
                return redirect(url_for("consulta_ventas"))

            # 🧱 DEDUP 3: heurística en BD (3 minutos) — se omite en pendiente (PASO 9)
            if not cobro_pendiente:
                cutoff = datetime.now() - timedelta(minutes=3)
                dup_q = (Venta.query
                         .filter(
                             Venta.Est_ID == est_id,
                             func.lower(Venta.Metodo_Pago) == metodo_norm.lower(),
                             (Venta.Referencia_Pago == (referencia_val if requires_reference(metodo_norm) else None)),
                             Venta.Fecha_Venta >= cutoff
                         )
                         .order_by(Venta.Fecha_Venta.desc()))
                if dup_q.first():
                    flash('Se detectó un registro reciente con el mismo método y referencia (se evitó un duplicado).', 'info')
                    return redirect(url_for("consulta_ventas"))

            # === Crear venta ===
            nueva_venta = Venta(
                Est_ID=est_id,
                Instructor_ID=None,
                Metodo_Pago=metodo_norm,  # "__pendiente__" si PASO 9
                Referencia_Pago=(referencia_val if (not cobro_pendiente and requires_reference(metodo_norm)) else None),
                Fecha_Venta=datetime.now(),
            )
            # PASO 9: si tu modelo tiene 'Estado', márcala como pendiente
            if cobro_pendiente and hasattr(nueva_venta, 'Estado'):
                try:
                    nueva_venta.Estado = 'pendiente'
                except Exception:
                    pass

            # Asociar pagos (conceptos) a la venta (solo referencia del concepto)
            total_conceptos_catalogo = 0.0
            for item in pagos_items:
                p = db.session.get(Pago, item["pago_id"])
                if p:
                    nueva_venta.pagos.append(p)
                    total_conceptos_catalogo += _to_float(p.Pago_Monto, 0.0) * int(item["qty"])
            total_conceptos_catalogo = money(total_conceptos_catalogo)  # NUEVO

            # Totales a cobrar (informativo)
            monto_cobrar_pagos = money(sum(i["charge_now"] for i in pagos_items))  # NUEVO
            monto_cobrar_planes = money(sum(m["monto"] for m in planes_movs))      # NUEVO
            _monto_cobrado_total = (0.0 if cobro_pendiente else money(monto_cobrar_pagos + monto_cobrar_planes))  # NUEVO

            # Persistir venta para obtener Venta_ID
            db.session.add(nueva_venta)
            db.session.flush()

            # === Persistencia por renglón (Planes/Abonos) ===
            for item in pagos_items:
                p_obj = db.session.get(Pago, item["pago_id"])
                if not p_obj:
                    continue

                if item["is_partial"]:
                    # PASO 9: no registrar abono si pendiente
                    plan, _created = get_or_create_plan(
                        est_id,
                        pago_obj=p_obj,
                        qty=item["qty"],
                        descripcion_resumen=None,
                        aplicar_descuento_al_liquidar=True,
                    )
                    if item["charge_now"] > 0 and not cobro_pendiente:
                        # NUEVO: tope por saldo_antes del plan (seguridad adicional)
                        try:
                            saldo_antes = float(getattr(plan, "Saldo_Actual", 0.0) or 0.0)
                        except Exception:
                            saldo_antes = 0.0
                        monto_a_registrar = min(item["charge_now"], saldo_antes)
                        monto_a_registrar = money(monto_a_registrar)
                        if monto_a_registrar <= 0:
                            continue
                        registrar_abono(
                            plan=plan,
                            venta=nueva_venta,
                            monto=monto_a_registrar,
                            metodo_norm=metodo_norm,
                            referencia=referencia_val,
                            observaciones="Parcial de concepto",
                            close_if_zero=False,
                        )
                else:
                    plan = find_open_plan(est_id, pago_id=item["pago_id"])
                    if plan and not cobro_pendiente:
                        neto_full_liq = money(float(item["full_breakdown"]["neto"] or 0.0))  # NUEVO
                        liquidar_plan(
                            plan=plan,
                            venta=nueva_venta,
                            neto_full=neto_full_liq,
                            metodo_norm=metodo_norm,
                            referencia=referencia_val,
                            observaciones="Liquidación automática (full en registro)",
                        )

            # Movimientos desde modal de Planes (PASO 9: omitidos si pendiente)
            for mov in planes_movs:
                plan = db.session.get(PlanCobro, mov["plan_id"])
                if not plan:
                    continue

                accion = (mov.get("accion") or "").strip().lower()
                if accion == "abonar":
                    if mov.get("monto", 0.0) > 0 and not cobro_pendiente:
                        # NUEVO: tope a saldo actual del plan
                        try:
                            saldo_antes = float(getattr(plan, "Saldo_Actual", 0.0) or 0.0)
                        except Exception:
                            saldo_antes = 0.0
                        monto_a_registrar = min(money(float(mov["monto"])), saldo_antes)
                        if monto_a_registrar <= 0:
                            continue
                        registrar_abono(
                            plan=plan,
                            venta=nueva_venta,
                            monto=monto_a_registrar,
                            metodo_norm=metodo_norm,
                            referencia=referencia_val,
                            observaciones="Abono manual (modal)",
                            close_if_zero=True,
                        )

                elif accion == "liquidar" and not cobro_pendiente:
                    pago_id = getattr(plan, "Pago_ID", None)
                    if pago_id:
                        meta = pagos_dict.get(pago_id, {})
                        unit = float(meta.get("monto", 0.0))
                        qty_guess = 1
                        try:
                            pb = float(getattr(plan, "Precio_Base_Snapshot", 0.0) or 0.0)
                            mt = float(getattr(plan, "Monto_Total_Original", 0.0) or 0.0)
                            if pb > 0.0:
                                qty_guess = max(1, int(round(mt / pb)))
                        except Exception:
                            qty_guess = 1

                        full_calc = compute_full_net(
                            unit_price=unit,
                            qty=qty_guess,
                            discount_pct=meta.get("descuento_porcentaje", 0.0),
                            discount_methods=meta.get("condiciones", []),
                            discount_valid_until=meta.get("valido_hasta"),
                            surcharge_pct=meta.get("recargo_porcentaje", 0.0),
                            surcharge_day_cut=meta.get("recargo_dia_corte", 0),
                            method_norm=metodo_norm,
                            today=today,
                            surcharge_on="post_discount",
                        )
                        neto_full = money(float(full_calc["neto"] or 0.0))  # NUEVO

                        liquidar_plan(
                            plan=plan,
                            venta=nueva_venta,
                            neto_full=neto_full,
                            metodo_norm=metodo_norm,
                            referencia=referencia_val,
                            observaciones="Liquidación desde modal de planes",
                        )
                    else:
                        saldo = money(float(getattr(plan, "Saldo_Actual", 0.0) or 0.0))  # NUEVO
                        if saldo > 0.0 and not cobro_pendiente:
                            registrar_abono(
                                plan=plan,
                                venta=nueva_venta,
                                monto=saldo,
                                metodo_norm=metodo_norm,
                                referencia=referencia_val,
                                observaciones="Liquidación por saldo (sin PAGO asociado)",
                                close_if_zero=True,
                            )

            # Registro de cobro (si aplica) — PASO 9: NO crear si pendiente
            if not cobro_pendiente and (monto_cobrar_pagos + monto_cobrar_planes) > 0 and VentaCobro:
                try:
                    cobro = VentaCobro(
                        Venta_ID=nueva_venta.Venta_ID,
                        Metodo=metodo_norm,
                        Referencia=(referencia_val if requires_reference(metodo_norm) else None),
                        Monto=money(monto_cobrar_pagos + monto_cobrar_planes),
                        Fecha=datetime.now()
                    )
                    db.session.add(cobro)
                except Exception as e:
                    current_app.logger.error("Error creando VentaCobro: %s", e)
                    current_app.logger.error(traceback.format_exc())

            # Commit final
            db.session.commit()

            # 🧱 DEDUP: Guardar llaves como "procesadas"
            if anti_dup_form_id:
                processed_forms.append(anti_dup_form_id)
                session['processed_forms'] = processed_forms[-100:]
            processed_keys.append(idem_key)
            session['processed_idem_keys'] = processed_keys[-100:]

            msg = "Registro guardado como PENDIENTE." if cobro_pendiente else "Abono(s)/pago(s) registrado(s) con éxito."
            flash(msg, "success")
            return redirect(url_for("consulta_ventas"))

        except IntegrityError as e:
            db.session.rollback()
            current_app.logger.error("IntegrityError en /registro/abonos: %s", e)
            current_app.logger.error(traceback.format_exc())
            error_flags["db"] = True
            flash("❌ Error de integridad en la base de datos. Verifique los datos e intente de nuevo.", "danger")
            return render_template(
                'registro_abonos.html',
                form=form,
                estudiantes=estudiantes,
                instructores=instructores,
                pagos_dict=pagos_dict,
                error_flags=error_flags,
                tipo_cliente_seleccionado="estudiante",
                pendientes=pendientes,
                planes_abiertos=[],
            )
        except Exception as e:
            db.session.rollback()
            # 🔎 Logging detallado: nombre de la excepción + traceback
            current_app.logger.error("Error en /registro/abonos [%s]: %s", type(e).__name__, e)
            current_app.logger.error(traceback.format_exc())
            error_flags["unexpected"] = True
            flash(f'❌ Error inesperado ({type(e).__name__}): {str(e)}', 'danger')
            return render_template(
                'registro_abonos.html',
                form=form,
                estudiantes=estudiantes,
                instructores=instructores,
                pagos_dict=pagos_dict,
                error_flags=error_flags,
                tipo_cliente_seleccionado="estudiante",
                pendientes=pendientes,
                planes_abiertos=[],
            )

    # === GET ===
    # Paso 8: cargar planes abiertos si llega ?est_id=
    planes_abiertos = []
    try:
        q_est_id = request.args.get('est_id')
        if q_est_id:
            q_est_id = int(q_est_id)
            if q_est_id > 0:
                planes_abiertos = (PlanCobro.query
                                   .filter(PlanCobro.Est_ID == q_est_id,
                                           func.lower(PlanCobro.Estado) == 'abierto',
                                           (PlanCobro.Saldo_Actual > 0))
                                   .order_by(getattr(PlanCobro, 'Fecha_Creacion', PlanCobro.Plan_ID).desc())
                                   .all())
    except Exception:
        planes_abiertos = []

    return render_template(
        'registro_abonos.html',
        form=form,
        estudiantes=estudiantes,
        instructores=instructores,
        pagos_dict=pagos_dict,
        error_flags={
            "cliente": False,
            "pagos": False,
            "abonos": False,
            "metodo_pago": False,
            "referencia": False,
            "db": False,
            "unexpected": False,
        },
        tipo_cliente_seleccionado="estudiante",
        pendientes=pendientes,
        planes_abiertos=planes_abiertos,  # 👈 ahora sí se envía al template
    )



########
# Planes abiertos
# === Nuevo endpoint ===
@app.route('/api/planes_abiertos')
def api_planes_abiertos():
    from flask import jsonify, request
    from sqlalchemy import func

    try:
        est_id = int(request.args.get('est_id', '0'))
    except Exception:
        est_id = 0

    if est_id <= 0:
        return jsonify({"ok": False, "planes": [], "error": "est_id inválido"}), 400

    from models import PlanCobro, Pago, Articulo, Paquete

    planes = (PlanCobro.query
              .filter(
                  PlanCobro.Est_ID == est_id,
                  func.lower(PlanCobro.Estado) == 'abierto',
                  (PlanCobro.Saldo_Actual.isnot(None)),
                  (PlanCobro.Saldo_Actual > 0)
              )
              .order_by(PlanCobro.Fecha_Creacion.desc())
              .all())

    def _tipo_item(p):
        if p.Articulo_ID is not None: return "articulo"
        if p.Paquete_ID  is not None: return "paquete"
        if p.Pago_ID     is not None: return "pago"
        return "desconocido"

    data = []
    for p in planes:
        data.append({
            "plan_id": p.Plan_ID,
            "tipo_item": _tipo_item(p),
            "descripcion": p.Descripcion_Resumen,
            "precio_base": float(p.Precio_Base_Snapshot or 0.0),
            "total": float(p.Monto_Total_Original or 0.0),
            "saldo": float(p.Saldo_Actual or 0.0),
            "fecha_creacion": p.Fecha_Creacion.isoformat() if p.Fecha_Creacion else None,
            "pago_id": p.Pago_ID,
        })

    return jsonify({"ok": True, "planes": data})

##############
## consulta abonos
##################
@app.route('/consulta/abonos')
def consulta_abonos():
    """
    Consulta de Abonos/Recibos con:
      - Filtros por estudiante, método, rango de fechas [desde, hasta] (hasta es inclusivo)
      - Totales (global y por página)
      - Paginación
      - Muestra Saldo_Antes, Monto_Abonado, Saldo_Despues, Método y Referencia
    """
    from datetime import datetime, timedelta
    from sqlalchemy import func, and_, or_, cast, Date
    from sqlalchemy.orm import joinedload, aliased

    # Models & helpers
    from models import Abono, PlanCobro, Pago, Estudiante, Venta
    try:
        # Para redondeo consistente a 2 decimales
        from billing_utils import money
    except Exception:
        # Fallback suave si no está disponible
        def money(x):
            try:
                return round(float(x or 0.0) + 1e-12, 2)
            except Exception:
                return 0.0

    # --------- Filtros (querystring) ---------
    est_id   = request.args.get('estudiante_id', type=int)
    est_id   = est_id if (est_id and est_id > 0) else None
    metodo   = (request.args.get('metodo', '') or '').strip().lower()
    f_desde  = (request.args.get('desde', '') or '').strip()
    f_hasta  = (request.args.get('hasta', '') or '').strip()
    per_page = max(1, min(200, request.args.get('per_page', default=25, type=int)))
    page     = max(1, request.args.get('page', default=1, type=int))

    # Catálogo de estudiantes (para selector)
    estudiantes = (Estudiante.query
                   .order_by(Estudiante.Est_Nombre.asc(), Estudiante.Est_ApellidoP.asc())
                   .all())

    # --------- Base query ---------
    V = aliased(Venta)
    q = (Abono.query
         .outerjoin(V, Abono.Venta_ID == V.Venta_ID)
         .outerjoin(PlanCobro, PlanCobro.Plan_ID == Abono.Plan_ID)
         .outerjoin(Estudiante, Estudiante.Est_ID == PlanCobro.Est_ID)
         .outerjoin(Pago, Pago.Pago_ID == PlanCobro.Pago_ID)
         .options(
             joinedload(Abono.plan).joinedload(PlanCobro.pago),
             joinedload(Abono.plan).joinedload(PlanCobro.estudiante),
             joinedload(Abono.venta)
         ))

    # Fecha “canónica” del abono: primero Fecha_Abono, si no, Fecha_Venta
    fecha_expr = func.coalesce(Abono.Fecha_Abono, V.Fecha_Venta)

    # --------- Aplicar filtros ---------
    if est_id:
        q = q.filter(or_(
            and_(Abono.Plan_ID.isnot(None), PlanCobro.Est_ID == est_id),
            and_(Abono.Venta_ID.isnot(None), V.Est_ID == est_id)
        ))

    if metodo:
        # método en Abono (lower) — si está vacío se muestra "—" en UI
        q = q.filter(func.lower(func.coalesce(Abono.Metodo_Pago, '')) == metodo)

    def _parse_date(s: str):
        try:
            return datetime.strptime(s, '%Y-%m-%d')
        except Exception:
            return None

    dt_desde = _parse_date(f_desde)
    dt_hasta = _parse_date(f_hasta)

    if dt_desde:
        # desde inclusivo (00:00)
        q = q.filter(fecha_expr >= dt_desde.replace(hour=0, minute=0, second=0, microsecond=0))
    if dt_hasta:
        # hasta inclusivo -> < (hasta + 1 día)
        end_exclusive = dt_hasta.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        q = q.filter(fecha_expr < end_exclusive)

    # --------- Conteos y totales ---------
    # Total de registros luego de filtros
    total_registros = q.count()

    # Total global de monto abonado (evita subquery compleja; compatible con SQLite)
    # Nota: usamos sum() en Python para 100% compat, en volúmenes altos podrías:
    # total_monto_global = (q.session.query(func.coalesce(func.sum(Abono.Monto_Abonado), 0.0))
    #                       .select_from(Abono)
    #                       .outerjoin(V, Abono.Venta_ID == V.Venta_ID)
    #                       .outerjoin(PlanCobro, PlanCobro.Plan_ID == Abono.Plan_ID)
    #                       .filter(*(q._criterion if hasattr(q, '_criterion') and q._criterion is not None else []))
    #                       .scalar())
    total_monto_global = 0.0
    for a in q.all():
        total_monto_global += float(a.Monto_Abonado or 0.0)
    total_monto_global = money(total_monto_global)

    # --------- Orden + paginación ---------
    q = q.order_by(fecha_expr.desc())
    page_rows = q.limit(per_page).offset((page - 1) * per_page).all()

    # --------- Armar filas (vista) ---------
    rows = []
    total_monto_pagina = 0.0

    for a in page_rows:
        plan = getattr(a, 'plan', None)
        est  = getattr(plan, 'estudiante', None) if plan else None
        pago = getattr(plan, 'pago', None) if plan else None
        v    = getattr(a, 'venta', None)

        # fecha mostrable (Date/DateTime)
        fecha_val = a.Fecha_Abono or (v.Fecha_Venta if v and getattr(v, 'Fecha_Venta', None) else None)

        saldo_antes   = money(getattr(a, 'Saldo_Antes', None))
        monto_abonado = money(getattr(a, 'Monto_Abonado', 0.0))
        saldo_despues = money(getattr(a, 'Saldo_Despues', None))

        total_monto_pagina += monto_abonado

        rows.append({
            "Abono_ID": getattr(a, 'Abono_ID', None),
            "Fecha": fecha_val,  # se renderiza con formateo en el template
            "Estudiante": (f"{getattr(est, 'Est_Nombre', '')} {getattr(est, 'Est_ApellidoP', '')}".strip() if est else "—"),
            "Concepto": getattr(pago, 'Pago_Tipo', '—'),
            "Saldo_Antes": saldo_antes,
            "Monto_Abonado": monto_abonado,
            "Saldo_Despues": saldo_despues,
            "Metodo_Pago": (getattr(a, 'Metodo_Pago', None) or '—'),
            "Referencia_Pago": (getattr(a, 'Referencia_Pago', None) or '—'),
            "Observaciones": (getattr(a, 'Observaciones', None) or ''),
        })

    total_monto_pagina = money(total_monto_pagina)

    # --------- Render ---------
    return render_template(
        'consulta_abonos.html',
        total=total_registros,
        total_monto_global=total_monto_global,
        total_monto_pagina=total_monto_pagina,
        rows=rows,
        estudiantes=estudiantes,
        filtros={
            "estudiante_id": est_id or "",
            "metodo": metodo or "",
            "desde": f_desde or "",
            "hasta": f_hasta or "",
            "per_page": per_page,
            "page": page,
        },
        pagination={
            "page": page,
            "per_page": per_page,
            "has_prev": page > 1,
            "has_next": (page * per_page) < total_registros,
            "prev_page": (page - 1),
            "next_page": (page + 1),
        }
    )


# ========= CONSULTA: PLANES =========
@app.route('/consulta/planes')
def consulta_planes():
    """
    Consulta de Planes de Cobro con:
      - Filtros por estudiante y estado (abierto | cerrado)
      - Totales globales: saldo en abiertos y monto cobrado en cerrados
      - Paginación y ordenamiento
    """
    from sqlalchemy import func, or_, and_
    from sqlalchemy.orm import joinedload
    from datetime import datetime

    # Extensiones / Modelos
    from extensions import db
    from models import PlanCobro, Estudiante, Pago

    # Redondeo consistente (fallback suave si no está disponible)
    try:
        from billing_utils import money
    except Exception:
        def money(x):
            try:
                return round(float(x or 0.0) + 1e-12, 2)
            except Exception:
                return 0.0

    # --------- Filtros (querystring) ---------
    est_id   = request.args.get('estudiante_id', type=int)
    est_id   = est_id if (est_id and est_id > 0) else None
    estado   = (request.args.get('estado', '') or '').strip().lower()  # '', 'abierto', 'cerrado'
    per_page = max(1, min(200, request.args.get('per_page', default=25, type=int)))
    page     = max(1, request.args.get('page', default=1, type=int))

    # Catálogo de estudiantes (selector)
    estudiantes = (Estudiante.query
                   .order_by(Estudiante.Est_Nombre.asc(), Estudiante.Est_ApellidoP.asc())
                   .all())

    # --------- Base query ---------
    q = (PlanCobro.query
         .options(
             joinedload(PlanCobro.estudiante),
             joinedload(PlanCobro.pago)
         ))

    if est_id:
        q = q.filter(PlanCobro.Est_ID == est_id)

    if estado in ('abierto', 'cerrado'):
        q = q.filter(func.lower(PlanCobro.Estado) == estado)

    # Ordenamiento: estado, saldo desc, más recientes primero (fallback si no hay Fecha_Creacion)
    try:
        order_date_col = getattr(PlanCobro, 'Fecha_Creacion')
    except Exception:
        order_date_col = PlanCobro.Plan_ID
    q = q.order_by(
        func.lower(PlanCobro.Estado).asc(),
        PlanCobro.Saldo_Actual.desc(),
        order_date_col.desc()
    )

    # --------- Totales globales ---------
    # Nota: para compatibilidad con SQLite y evitar subconsultas complejas, usamos subquery simple
    try:
        base_sq = q.with_entities(
            PlanCobro.Plan_ID.label('Plan_ID'),
            PlanCobro.Estado.label('Estado'),
            PlanCobro.Monto_Total_Original.label('Monto_Total_Original'),
            PlanCobro.Saldo_Actual.label('Saldo_Actual')
        ).subquery()

        total_registros = db.session.query(func.count(base_sq.c.Plan_ID)).scalar() or 0

        tot_abiertos = db.session.query(
            func.coalesce(func.sum(base_sq.c.Saldo_Actual), 0.0)
        ).filter(func.lower(base_sq.c.Estado) == 'abierto').scalar() or 0.0

        tot_cobrados_cerrados = db.session.query(
            func.coalesce(func.sum(base_sq.c.Monto_Total_Original - base_sq.c.Saldo_Actual), 0.0)
        ).filter(func.lower(base_sq.c.Estado) == 'cerrado').scalar() or 0.0

    except Exception:
        # Fallback: traer y sumar en Python (menos eficiente, pero robusto)
        all_rows = q.all()
        total_registros = len(all_rows)
        tot_abiertos = sum(float(getattr(p, 'Saldo_Actual', 0.0) or 0.0)
                           for p in all_rows if str(getattr(p, 'Estado', '')).lower() == 'abierto')
        tot_cobrados_cerrados = sum(
            float(getattr(p, 'Monto_Total_Original', 0.0) or 0.0) - float(getattr(p, 'Saldo_Actual', 0.0) or 0.0)
            for p in all_rows if str(getattr(p, 'Estado', '')).lower() == 'cerrado'
        )

    # --------- Paginación ---------
    page_rows = q.limit(per_page).offset((page - 1) * per_page).all()

    # --------- Armar filas (vista) ---------
    rows = []
    for p in page_rows:
        est  = getattr(p, 'estudiante', None)
        pago = getattr(p, 'pago', None)

        rows.append({
            "Plan_ID": getattr(p, 'Plan_ID', None),
            "Estudiante": (f"{getattr(est, 'Est_Nombre', '')} {getattr(est, 'Est_ApellidoP', '')}".strip() if est else "—"),
            "Concepto": getattr(pago, 'Pago_Tipo', '—'),
            "Monto_Total_Original": money(getattr(p, 'Monto_Total_Original', 0.0)),
            "Saldo_Actual": money(getattr(p, 'Saldo_Actual', 0.0)),
            "Estado": getattr(p, 'Estado', '—'),
            "Vigencia_Inicio": getattr(p, 'Vigencia_Inicio', None),
            "Vigencia_Fin": getattr(p, 'Vigencia_Fin', None),
        })

    # --------- Render ---------
    return render_template(
        'consulta_planes.html',
        total=total_registros,
        totales={
            "abiertos": money(tot_abiertos),
            "cerrados": money(tot_cobrados_cerrados)  # lo cobrado en planes cerrados
        },
        rows=rows,
        estudiantes=estudiantes,
        filtros={
            "estudiante_id": est_id or "",
            "estado": estado or "",
            "per_page": per_page,
            "page": page,
        },
        pagination={
            "page": page,
            "per_page": per_page,
            "has_prev": page > 1,
            "has_next": (page * per_page) < total_registros,
            "prev_page": (page - 1),
            "next_page": (page + 1),
        }
    )

# === RUTAS: eliminar abonos y eliminar planes ================================
# Reglas sugeridas:
# - Eliminar Abono:
#     * Requiere POST.
#     * Si el Abono pertenece a un Plan, se "reversa" el saldo: Saldo_Actual += Monto_Abonado.
#     * Si el Plan estaba 'cerrado' y tras la reversa el saldo > 0, se reabre ('abierto').
#     * No toca VentaCobro (si tienes esa tabla, considera manejarlo en tu flujo contable).
# - Eliminar Plan:
#     * Por defecto, solo si NO tiene abonos (histórico limpio) y sin importar el saldo.
#     * Si agregas ?force=1, elimina primero TODOS los abonos del plan y luego el plan (¡pierdes historial!).
#       Úsalo con cautela o deshabilítalo en producción.
# - Redirige al referer (si existe) o a las consultas por defecto.
#
# Nota: Estas rutas asumen que usas CSRF (Flask-WTF) en los formularios que hacen POST.

from flask import request, redirect, url_for, flash
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from extensions import db
from models import Abono, PlanCobro

# Redondeo consistente a 2 decimales (fallback suave)
try:
    from billing_utils import money
except Exception:
    def money(x):
        try:
            return round(float(x or 0.0) + 1e-12, 2)
        except Exception:
            return 0.0


def _safe_redirect(default_endpoint: str):
    """Redirige de vuelta al referer si existe; si no, al endpoint indicado."""
    ref = (request.referrer or '').strip()
    if ref:
        return redirect(ref)
    return redirect(url_for(default_endpoint))


@app.route('/abonos/<int:abono_id>/eliminar', methods=['POST'])
def eliminar_abono(abono_id: int):
    """
    Elimina un Abono y, si está ligado a un Plan:
      - Revierte su efecto en el Plan: Saldo_Actual += Monto_Abonado
      - Si el plan estaba 'cerrado' y queda Saldo_Actual > 0, se reabre ('abierto')
    """
    try:
        ab = (Abono.query
              .options(joinedload(Abono.plan))
              .filter(Abono.Abono_ID == abono_id)
              .first())
        if not ab:
            flash('Abono no encontrado.', 'warning')
            return _safe_redirect('consulta_abonos')

        monto = money(getattr(ab, 'Monto_Abonado', 0.0))
        plan = getattr(ab, 'plan', None)

        if plan is not None:
            saldo_antes = money(getattr(plan, 'Saldo_Actual', 0.0))
            plan.Saldo_Actual = money(saldo_antes + monto)

            # Si estaba cerrado y ahora tiene saldo, se reabre
            estado = (getattr(plan, 'Estado', '') or '').strip().lower()
            if estado == 'cerrado' and plan.Saldo_Actual > 0:
                plan.Estado = 'abierto'

        db.session.delete(ab)
        db.session.commit()

        if plan is not None:
            flash(f'Abono eliminado. Plan #{plan.Plan_ID} actualizado (saldo: ${plan.Saldo_Actual:.2f}).', 'success')
        else:
            flash('Abono eliminado.', 'success')

        return _safe_redirect('consulta_abonos')

    except IntegrityError as e:
        db.session.rollback()
        flash('❌ No se pudo eliminar el abono (conflicto de integridad).', 'danger')
        return _safe_redirect('consulta_abonos')
    except Exception as e:
        db.session.rollback()
        flash(f'❌ Error inesperado al eliminar abono: {type(e).__name__}', 'danger')
        return _safe_redirect('consulta_abonos')


@app.route('/planes/<int:plan_id>/eliminar', methods=['POST'])
def eliminar_plan(plan_id: int):
    """
    Elimina un Plan de Cobro.
    Comportamiento:
      - Por defecto: solo permite eliminar si el plan NO tiene abonos.
      - Si querystring incluye ?force=1: elimina TODOS los abonos del plan y luego el plan (pierde historial).
        Úsalo con extrema cautela.

    Recomendación: En producción, deshabilita el 'force' si necesitas auditoría.
    """
    force = str(request.args.get('force', '') or '').strip() == '1'

    try:
        plan = (PlanCobro.query
                .options(joinedload(PlanCobro.estudiante), joinedload(PlanCobro.pago))
                .filter(PlanCobro.Plan_ID == plan_id)
                .first())
        if not plan:
            flash('Plan no encontrado.', 'warning')
            return _safe_redirect('consulta_planes')

        # Verificar si existen abonos ligados
        abonos_q = Abono.query.filter(Abono.Plan_ID == plan.Plan_ID)
        abonos_count = abonos_q.count()

        if abonos_count > 0 and not force:
            flash('No se puede eliminar el plan: tiene abonos registrados. '
                  'Si estás absolutamente seguro, usa la opción forzada.', 'warning')
            return _safe_redirect('consulta_planes')

        if force and abonos_count > 0:
            # ⚠️ ELIMINACIÓN DE HISTORIAL
            for ab in abonos_q.all():
                db.session.delete(ab)

        db.session.delete(plan)
        db.session.commit()

        if force and abonos_count > 0:
            flash(f'Plan #{plan_id} y {abonos_count} abono(s) fueron eliminados permanentemente.', 'success')
        else:
            flash(f'Plan #{plan_id} eliminado.', 'success')

        return _safe_redirect('consulta_planes')

    except IntegrityError:
        db.session.rollback()
        flash('❌ No se pudo eliminar el plan (conflicto de integridad).', 'danger')
        return _safe_redirect('consulta_planes')
    except Exception as e:
        db.session.rollback()
        flash(f'❌ Error inesperado al eliminar plan: {type(e).__name__}', 'danger')
        return _safe_redirect('consulta_planes')



if __name__ == '__main__':
    app.run(debug=True)