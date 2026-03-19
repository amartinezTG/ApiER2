# conceptos.py
"""
Extracción de conceptos/detalles de facturas por proveedor
"""
import re
import fitz  # PyMuPDF
import pdfplumber
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any, List

from .utils import _strip_diacritics, _to_dec, _clean


def extraer_conceptos_lobo_por_tabla(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Lee la tabla CONCEPTOS con pdfplumber mapeando columnas por encabezado.
    """
    conceptos: List[Dict[str, Any]] = []
    try:
        import pdfplumber
    except Exception:
        return conceptos

    def norm(s: Any) -> str:
        return _strip_diacritics((s or "")).strip().upper()

    header_alias = {
        "CANTIDAD": "Cantidad",
        "UNIDAD": "Unidad",
        "NO. IDENTIFICACION": "NoIdentificacion",
        "NO. IDENTIFICACIÓN": "NoIdentificacion",
        "NO IDENTIFICACION": "NoIdentificacion",
        "NO IDENTIFICACIÓN": "NoIdentificacion",
        "DESCRIPCION": "Descripcion",
        "DESCRIPCIÓN": "Descripcion",
        "PRECIO UNITARIO": "ValorUnitario",
        "OBJETO IMP.": "ObjetoImp",
        "OBJETO IMP": "ObjetoImp",
        "IMPORTE": "Importe",
    }

    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            # Intenta extraer tablas con estrategia por líneas
            tables = page.extract_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_x_tolerance": 5,
                "intersection_y_tolerance": 5,
                "snap_tolerance": 3,
                "join_tolerance": 3,
                "edge_min_length": 40,
            }) or page.extract_tables()

            for table in (tables or []):
                if not table or all(all((c is None or str(c).strip() == "") for c in row) for row in table):
                    continue

                # 1) Detecta fila de encabezados
                header_row_idx = None
                col_map: Dict[int, str] = {}

                for idx, row in enumerate(table[:5]):  # mira primeras filas
                    labels = [norm(c) for c in row]
                    hits = 0
                    for j, lab in enumerate(labels):
                        for key in list(header_alias.keys()):
                            if key in lab:
                                col_map[j] = header_alias[key]
                                hits += 1
                    # Considera header si halló al menos 3 columnas relevantes
                    if hits >= 3:
                        header_row_idx = idx
                        break

                if header_row_idx is None or not col_map:
                    continue  # no parece ser la tabla de conceptos

                # 2) Recorre las filas de datos
                for r in table[header_row_idx + 1:]:
                    row = [(c or "").strip() for c in r]
                    if all(c == "" for c in row):
                        continue

                    # Si la fila no tiene nada relevante, sáltala
                    row_norm = " ".join(row).upper()
                    if not any(k in row_norm for k in ["GASOLINA", "DIESEL", "LTR", "$", ","]):
                        continue

                    # Lectura por columnas
                    raw = {}
                    for j, colname in col_map.items():
                        raw[colname] = row[j] if j < len(row) else ""

                    # --- Parseo de columnas principales ---
                    cantidad = _to_dec(raw.get("Cantidad"))
                    unidad = raw.get("Unidad") or "LTR"
                    no_id = raw.get("NoIdentificacion") or ""
                    desc = raw.get("Descripcion") or ""
                    valor_unitario = _to_dec(raw.get("ValorUnitario"), prec=6)

                    # Objeto Imp.: "02 - Si objeto..." -> '02'
                    objeto_imp = ""
                    obj = raw.get("ObjetoImp") or ""
                    m = re.search(r'(\d{2})', obj)
                    if m:
                        objeto_imp = m.group(1)
                    else:
                        objeto_imp = "02"

                    # Importe
                    importe = _to_dec(raw.get("Importe"), prec=2)

                    # Si la cantidad viene repetida en dos renglones
                    if cantidad == 0:
                        joined = " ".join([raw.get("Cantidad",""), raw.get("Unidad","")])
                        m = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d{3,6})?)', joined)
                        if m:
                            cantidad = _to_dec(m.group(1))

                    # --- Datos fiscales en la DESCRIPCIÓN ---
                    # Clave Prod. Serv.
                    m_cps = re.search(r'CLAVE\s*PROD\.?\s*SERV\.?\s*-\s*(\d+)', _strip_diacritics(desc), re.I)
                    cps = m_cps.group(1) if m_cps else None

                    # IVA Base - xxx  Tasa - 0.160000  Importe - $ yyy
                    desc_norm = _strip_diacritics(desc)
                    m_base = re.search(r'IVA\s*BASE\s*-\s*([0-9][\d,]*\.\d+)', desc_norm, re.I)
                    m_tasa = re.search(r'TASA\s*-\s*([0-9]+\.\d+)', desc_norm, re.I)
                    m_iva  = re.search(r'IMPORTE\s*-\s*\$\s*([0-9][\d,]*\.\d+)', desc_norm, re.I)

                    base = _to_dec(m_base.group(1)) if m_base else None
                    tasa = _to_dec(m_tasa.group(1)) if m_tasa else None
                    iva_importe = _to_dec(m_iva.group(1), prec=6) if m_iva else None

                    # Defaults coherentes si faltan
                    if not valor_unitario and cantidad and importe:
                        valor_unitario = (importe / cantidad).quantize(Decimal('1.000000'))
                    if not importe and cantidad and valor_unitario:
                        importe = (cantidad * valor_unitario).quantize(Decimal('1.00'))

                    if base is None:
                        base = Decimal(importe) if isinstance(importe, Decimal) else Decimal('0')
                    if tasa is None:
                        tasa = Decimal('0.160000')  # 16% por defecto
                    else:
                        # si vino como 16.000000 (%), conviértelo a tasa 0.16
                        if tasa > 1:
                            tasa = (tasa / Decimal('100')).quantize(Decimal('1.000000'))

                    if iva_importe is None:
                        iva_importe = (base * tasa).quantize(Decimal('1.000000'))

                    conceptos.append({
                        "Cantidad": cantidad.quantize(Decimal('1.0000')) if isinstance(cantidad, Decimal) else Decimal('0.0000'),
                        "ClaveProdServ": cps,
                        "ClaveUnidad": "LTR",
                        "Descripcion": desc,
                        "ValorUnitario": valor_unitario.quantize(Decimal('1.000000')) if isinstance(valor_unitario, Decimal) else Decimal('0.000000'),
                        "Importe": importe.quantize(Decimal('1.00')) if isinstance(importe, Decimal) else Decimal('0.00'),
                        "NoIdentificacion": no_id,
                        "ObjetoImp": objeto_imp,
                        "Impuesto": "IVA",
                        "TasaOCuota": tasa.quantize(Decimal('1.000000')) if isinstance(tasa, Decimal) else Decimal('0.160000'),
                        "TipoFactor": "Tasa",
                        "Base": base.quantize(Decimal('1.000000')) if isinstance(base, Decimal) else Decimal('0.000000'),
                        "Unidad": "LTR",
                        "ImporteImpuesto": iva_importe.quantize(Decimal('1.000000')) if isinstance(iva_importe, Decimal) else Decimal('0.000000'),
                    })

    return conceptos

def extraer_conceptos_lobo_por_regex(doc: fitz.Document) -> List[Dict[str, Any]]:
    """
    Respaldo cuando no hay tablas legibles. Usa regex sobre el texto corrido.
    """
    conceptos: List[Dict[str, Any]] = []

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))
    
    # Cantidad + LTR
    m_cant = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d{3})?)\s*(?:000)?\s*LTR', full_text, re.I)
    cantidad = _to_dec(m_cant.group(1)) if m_cant else Decimal('0')

    # NoIdentificacion (después de LTR )
    m_noid = re.search(r'\bLTR\s+([A-Z0-9/.-]+)\b', full_text, re.I)
    no_id = _clean(m_noid.group(1)) if m_noid else ""

    # Descripción (combustibles)
    m_desc = re.search(r'(GASOLINA\s+[A-Z\s()]+|DIESEL\s+[A-Z\s()]+)', full_text, re.I)
    desc = _clean(m_desc.group(1)) if m_desc else ""

    # ClaveProdServ
    m_cps = re.search(r'Clave\s*Prod\.?\s*Serv\.?\s*-\s*(\d+)', full_text, re.I)
    cps = _clean(m_cps.group(1)) if m_cps else None

    # Valor unitario
    m_vu = re.search(r'\$\s*([0-9][\d,]*\.\d{2,6})\s+0?2\s*-\s*Si\s*objeto', full_text, re.I)
    vunit = _to_dec(m_vu.group(1)) if m_vu else Decimal('0')

    # Importe
    m_imp = re.search(r'(?<!SUB)\bTOTAL\b\s+\$\s*([0-9][\d,]*\.\d+)', full_text, re.I)
    importe_total = _to_dec(m_imp.group(1), prec=2) if m_imp else None
    importe = (cantidad * vunit).quantize(Decimal('1.00')) if (cantidad and vunit) else None

    # ObjetoImp y IVA
    m_obj = re.search(r'(\b0?2\b)\s*-\s*Si\s*objeto', full_text, re.I)
    objeto = m_obj.group(1) if m_obj else '02'

    # IVA base / tasa / importe
    m_base = re.search(r'IVA\s*Base\s*-\s*([0-9][\d,]*\.\d+)', full_text, re.I)
    m_tasa = re.search(r'Tasa\s*-\s*([0-9]+\.\d+)', full_text, re.I)
    m_ivai = re.search(r'IVA\s*Base[^\$]+\$\s*([0-9][\d,]*\.\d+)', full_text, re.I)

    base = _to_dec(m_base.group(1)) if m_base else (importe or Decimal('0'))
    tasa = _to_dec(m_tasa.group(1)) if m_tasa else Decimal('16.0')  # % → luego /100
    iva_importe = _to_dec(m_ivai.group(1)) if m_ivai else ((base * (tasa/Decimal('100'))).quantize(Decimal('1.000000')) if base else Decimal('0'))

    # Arma 1 concepto (LOBO usualmente maneja 1 renglón por PDF)
    conceptos.append({
        "Cantidad": cantidad or Decimal('0'),
        "ClaveProdServ": cps,
        "ClaveUnidad": "LTR",
        "Descripcion": desc,
        "ValorUnitario": vunit or Decimal('0'),
        "Importe": (importe or Decimal('0')) if importe else (importe_total or Decimal('0')),
        "NoIdentificacion": no_id,
        "ObjetoImp": objeto,
        "Impuesto": "IVA",
        "TasaOCuota": (tasa/Decimal('100')).quantize(Decimal('1.000000')),  # 0.160000
        "TipoFactor": "Tasa",
        "Base": base.quantize(Decimal('1.000000')) if isinstance(base, Decimal) else Decimal('0'),
        "Unidad": "LTR",
        "ImporteImpuesto": iva_importe.quantize(Decimal('1.000000')) if isinstance(iva_importe, Decimal) else Decimal('0'),
    })

    return conceptos

def extraer_conceptos_lobo(path_pdf: Path) -> List[Dict[str, Any]]:
    """Intenta tabla (pdfplumber) y si no sale, usa regex sobre texto (PyMuPDF)."""
    conceptos = extraer_conceptos_lobo_por_tabla(path_pdf)
    if conceptos:
        # Completa campos fiscales si faltaron
        for c in conceptos:
            c["ObjetoImp"] = c.get("ObjetoImp") or '02'
            c["Impuesto"] = c.get("Impuesto") or 'IVA'
            c["TipoFactor"] = c.get("TipoFactor") or 'Tasa'
            if not c.get("TasaOCuota"):
                c["TasaOCuota"] = Decimal('0.160000')
            if not c.get("Base"):
                c["Base"] = (c.get("Importe") or Decimal('0')).quantize(Decimal('1.000000'))
            if not c.get("ImporteImpuesto"):
                c["ImporteImpuesto"] = (c["Base"] * c["TasaOCuota"]).quantize(Decimal('1.000000'))
            c["ClaveUnidad"] = c.get("ClaveUnidad") or 'LTR'
            c["Unidad"] = c.get("Unidad") or 'LTR'
        return conceptos

    # Respaldo: regex sobre el texto
    with fitz.open(str(path_pdf)) as doc:
        return extraer_conceptos_lobo_por_regex(doc)

def extraer_conceptos_mcg_desde_cadena(full_text: str) -> List[Dict[str, Any]]:
    """
    Intenta extraer conceptos desde la Cadena Original en MCG (muy estructurada).
    CORREGIDO: Extrae de la cadena original del SAT, no duplica conceptos.
    """
    cpts: List[Dict[str, Any]] = []
    t = _strip_diacritics(full_text)

    # Buscar la cadena original del SAT que tiene la estructura exacta
    cadena_match = re.search(r'Cadena original del complemento de certificacion digital del SAT:\|\|([^|]+(?:\|[^|]*)*)', t, re.I)
    if not cadena_match:
        return []

    cadena_original = cadena_match.group(1)
    
    # Dividir la cadena en sus componentes usando | como separador
    componentes = cadena_original.split('|')
    
    # Encontrar los conceptos en la cadena original
    # Patrón para conceptos: cantidad|unidad|descripción|precio_unitario|importe|objeto_imp|base|impuesto|tipo_factor|tasa|importe_impuesto
    
    i = 0
    while i < len(componentes):
        # Buscar patrón de concepto: número decimal seguido de unidad (LTR, ACT, etc)
        if i + 10 < len(componentes):  # Necesitamos al menos 11 elementos para un concepto completo
            try:
                cantidad_str = componentes[i].strip()
                unidad = componentes[i + 1].strip()
                descripcion = componentes[i + 2].strip()
                precio_unitario_str = componentes[i + 3].strip()
                importe_str = componentes[i + 4].strip()
                
                # Verificar si es un concepto válido
                if (re.match(r'^\d+\.?\d*$', cantidad_str) and 
                    unidad in ['LTR', 'ACT', 'H87', 'E48'] and 
                    re.match(r'^\d+\.?\d*$', precio_unitario_str) and
                    re.match(r'^\d+\.?\d*$', importe_str)):
                    
                    cantidad = _to_dec(cantidad_str, prec=4)
                    precio_unitario = _to_dec(precio_unitario_str, prec=6)
                    importe = _to_dec(importe_str, prec=2)
                    
                    # Los siguientes campos están en posiciones fijas después del importe
                    objeto_imp = componentes[i + 5].strip() if i + 5 < len(componentes) else "02"
                    base_str = componentes[i + 6].strip() if i + 6 < len(componentes) else "0"
                    impuesto_code = componentes[i + 7].strip() if i + 7 < len(componentes) else "002"
                    tipo_factor = componentes[i + 8].strip() if i + 8 < len(componentes) else "Tasa"
                    tasa_str = componentes[i + 9].strip() if i + 9 < len(componentes) else "0.080000"
                    importe_impuesto_str = componentes[i + 10].strip() if i + 10 < len(componentes) else "0"
                    
                    base = _to_dec(base_str, prec=6)
                    tasa = _to_dec(tasa_str, prec=6)
                    importe_impuesto = _to_dec(importe_impuesto_str, prec=6)
                    
                    # Normalizar unidad
                    if unidad in ("L", "LT"):
                        unidad = "LTR"
                    
                    # Crear el concepto
                    concepto = {
                        "Cantidad": cantidad,
                        "ClaveProdServ": None,
                        "ClaveUnidad": unidad,
                        "Descripcion": descripcion,
                        "ValorUnitario": precio_unitario,
                        "Importe": importe,
                        "NoIdentificacion": "",
                        "ObjetoImp": objeto_imp,
                        "Impuesto": "IVA",
                        "TasaOCuota": tasa,
                        "TipoFactor": tipo_factor,
                        "Base": base,
                        "Unidad": unidad,
                        "ImporteImpuesto": importe_impuesto,
                    }
                    
                    cpts.append(concepto)
                    
                    # Avanzar después de procesar este concepto
                    i += 11
                else:
                    i += 1
            except (ValueError, IndexError):
                i += 1
        else:
            i += 1

    return cpts

def extraer_conceptos_mcg_por_tabla(path_pdf: Path) -> List[Dict[str, Any]]:
    """Extrae conceptos de MCG usando pdfplumber - CORREGIDA."""
    cpts: List[Dict[str, Any]] = []
    try:
        import pdfplumber
    except Exception:
        return cpts

    alias = {
        "ENTREGA/REFERENCIA": "ref",
        "VOLUMEN": "Cantidad",
        "UNIDAD": "Unidad", 
        "CONCEPTO": "Descripcion",
        "PRECIO": "ValorUnitario",
        "IMPORTE": "Importe",
        "MONEDA": "Moneda",
    }

    def N(s): return _strip_diacritics((s or "")).strip().upper()

    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables({"vertical_strategy":"lines","horizontal_strategy":"lines"}) or page.extract_tables()
            for t in (tables or []):
                if not t: 
                    continue
                
                # Localiza header
                header = None; colmap={}
                for i,row in enumerate(t[:5]):
                    labs = [N(c) for c in row]
                    for j,lab in enumerate(labs):
                        for k,v in alias.items():
                            if k in lab:
                                colmap[j]=v
                    if len(colmap)>=3:
                        header=i; break
                if header is None: 
                    continue

                for row in t[header+1:]:
                    cells=[(c or "").strip() for c in row]
                    
                    # Descarta filas de totales y servicios que no son conceptos principales
                    line_up = N(" ".join(cells))
                    if any(x in line_up for x in ["SUBTOTAL","TOTAL","IMPUESTO","CONDICION COMERCIAL", "CONDICIÓN COMERCIAL", "IVASERVICIO", "SERVICIOASOCIADO"]):
                        continue
                    
                    # Arma dict
                    d={}
                    for j,name in colmap.items():
                        d[name]=cells[j] if j<len(cells) else ""
                    
                    if not d.get("Descripcion") and not d.get("ValorUnitario") and not d.get("Importe"):
                        continue

                    # Solo procesar conceptos principales (combustible)
                    descripcion = d.get("Descripcion", "").upper()
                    if not any(combustible in descripcion for combustible in ["MAGNA", "PREMIUM", "DIESEL", "GASOLINA"]):
                        continue

                    qty = _to_dec(d.get("Cantidad"), prec=4)
                    unit = (d.get("Unidad") or "").upper()
                    unit = "LTR" if unit in ("L","LT","LTR") else unit or "LTR"
                    punit = _to_dec(d.get("ValorUnitario"), prec=6)
                    imp = _to_dec(d.get("Importe"), prec=2)
                    desc = d.get("Descripcion") or ""

                    # Calcular base e impuestos
                    base = imp if imp else (qty*punit if qty and punit else Decimal('0'))
                    
                    # Para MCG, determinar la tasa basada en el tipo de producto
                    # Revisar si en el texto completo se especifica la tasa
                    page_text = page.extract_text() or ""
                    if "TASA 0.080000" in page_text.upper() or "0.080000" in page_text:
                        tasa = Decimal('0.080000')
                    else:
                        tasa = Decimal('0.160000')  # Default
                    
                    iva = (base * tasa).quantize(Decimal('1.000000')) if base else Decimal('0')

                    cpts.append({
                        "Cantidad": qty, "ClaveProdServ": None, "ClaveUnidad": unit,
                        "Descripcion": desc, "ValorUnitario": punit, "Importe": imp,
                        "NoIdentificacion": "", "ObjetoImp": "02", "Impuesto": "IVA",
                        "TasaOCuota": tasa, "TipoFactor": "Tasa", "Base": base.quantize(Decimal('1.000000')),
                        "Unidad": unit, "ImporteImpuesto": iva
                    })
    return cpts

def extraer_conceptos_mcg(path_pdf: Path) -> List[Dict[str, Any]]:
    """Extrae conceptos de MCG: intenta desde Cadena original, luego tabla."""
    # 1) Intenta desde la Cadena original (muy exacta en MCG)
    with fitz.open(str(path_pdf)) as doc:
        full = " ".join([p.get_text() or "" for p in doc])
    c = extraer_conceptos_mcg_desde_cadena(full)
    if c:
        return c
    # 2) Si no, intenta tabla
    c = extraer_conceptos_mcg_por_tabla(path_pdf)
    return c

def extraer_conceptos_tesoro(path_pdf: Path) -> List[Dict[str, Any]]:
    cpts: List[Dict[str, Any]] = []
    try:
        import pdfplumber
    except Exception:
        return cpts

    alias = {
        "CANTIDAD": "Cantidad",
        "CLAVE PROD/SERV": "ClaveProdServ",
        "CLAVE": "ClaveProdServ",  # por si separan la palabra
        "CLAVE UNIDAD": "ClaveUnidad",
        "UNIDAD DE MEDIDA": "Unidad",
        "DESCRIPCION": "Descripcion",
        "DESCRIPCIÓN": "Descripcion",
        "OBJETO IMP": "ObjetoImp",
        "VALOR UNITARIO": "ValorUnitario",
        "IMPORTE": "Importe",
    }

    def N(s): return _strip_diacritics((s or "")).strip().upper()

    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = (page.extract_text() or "")
            tables = page.extract_tables({"vertical_strategy":"lines","horizontal_strategy":"lines"}) or page.extract_tables()
            for t in (tables or []):
                if not t:
                    continue

                # localizar header flexible (permitir encabezados partidos)
                header, colmap = None, {}
                for i, row in enumerate(t[:6]):
                    labs = [N(c) for c in row]
                    # Intento 1: celda directa
                    for j, lab in enumerate(labs):
                        for k, v in alias.items():
                            if k in lab:
                                colmap[j] = v
                    # Intento 2: concatenar celdas vecinas para capturar 'VALOR' + 'UNITARIO'
                    joined = " ".join(labs)
                    for k, v in alias.items():
                        if k in joined:
                            # Marca al menos una columna del bloque como presente
                            # (no necesitamos la posición exacta para vunit si luego corroboramos por Addenda)
                            colmap[-1] = v  # pseudo-columna para 'presente'
                    if len(colmap) >= 4:
                        header = i
                        break

                if header is None:
                    continue

                for row in t[header+1:]:
                    cells = [(c or "").strip() for c in row]
                    if not any(cells):
                        continue

                    up = N(" ".join(cells))
                    if any(w in up for w in ["SUBTOTAL", "TOTAL", "IMPUEST", "RETENID", "DESCUENTO"]):
                        continue

                    d = {}
                    for j, name in colmap.items():
                        if j == -1:
                            continue
                        if j < len(cells):
                            d[name] = cells[j]

                    # Filtra líneas que no sean combustible
                    desc = (d.get("Descripcion") or "").upper()
                    if not any(x in desc for x in ["DIESEL", "GASOLINA", "MAGNA", "PREMIUM", "UNBRANDED"]):
                        continue

                    # ---------- Reconstrucción robusta ----------
                    cantidad = _to_dec(d.get("Cantidad"), prec=4)
                    vunit    = _to_dec(d.get("ValorUnitario"), prec=6)
                    importe  = _to_dec(d.get("Importe"), prec=2)

                    # Respaldo 1: Si NO hay ValorUnitario en tabla, buscarlo en Addenda (texto de página)
                    if vunit == 0:
                        m = re.search(r'Valor\s+Unitario\s+([0-9,]+\.\d+)', page_text, re.I)
                        if m:
                            vunit = _to_dec(m.group(1), prec=6)

                    # Respaldo 2: Si sigue en 0 pero tienes cantidad e importe, calcula vunit = importe/cantidad
                    if vunit == 0 and cantidad > 0 and importe > 0:
                        from decimal import Decimal
                        vunit = (importe / cantidad).quantize(Decimal('1.000000'))

                    # Respaldo 3: Si el importe vino 0 pero ya tienes cantidad y vunit, calcúlalo
                    if importe == 0 and cantidad > 0 and vunit > 0:
                        from decimal import Decimal
                        importe = (cantidad * vunit).quantize(Decimal('1.00'))

                    # Normaliza claves de unidad
                    clave_unidad = (d.get("ClaveUnidad") or "LTR").upper()
                    unidad = (d.get("Unidad") or "LTR").upper()
                    if clave_unidad in ("L", "LT"): clave_unidad = "LTR"
                    if unidad in ("L", "LT"): unidad = "LTR"

                    # Objeto de impuesto
                    objeto = "02"
                    if "OBJETO IMP" in colmap.values():
                        m = re.search(r'(\d{2})', d.get("ObjetoImp") or "")
                        objeto = m.group(1) if m else "02"

                    # Base e IVA
                    from decimal import Decimal
                    base = importe if importe > 0 else (cantidad * vunit)
                    tasa = Decimal('0.160000')
                    iva  = (base * tasa).quantize(Decimal('1.000000'))

                    cpts.append({
                        "Cantidad": cantidad,
                        "ClaveProdServ": d.get("ClaveProdServ") or "15101505",
                        "ClaveUnidad": clave_unidad,
                        "Descripcion": d.get("Descripcion") or "",
                        "ValorUnitario": vunit,             # <- ya NO quedará en 0
                        "Importe": importe,
                        "NoIdentificacion": "",
                        "ObjetoImp": objeto,
                        "Impuesto": "IVA",
                        "TasaOCuota": tasa,
                        "TipoFactor": "Tasa",
                        "Base": base.quantize(Decimal('1.000000')),
                        "Unidad": unidad,
                        "ImporteImpuesto": iva
                    })
    return cpts

def extraer_conceptos_aemsa(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Extrae conceptos de facturas AEMSA usando regex del texto plano.
    
    Formato de línea en AEMSA:
    15459 15101514 LTR GASOLINA REGULAR 02 $18.914799 $292,403.88 $45,375.27
    CANT  CLAVE    UDM CONCEPTO         OBJ PREC.UNIT  IMPORTE     IVA
    """
    cpts: List[Dict[str, Any]] = []
    
    try:
        import pdfplumber
    except Exception:
        return cpts
    
    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            
            # Buscar líneas que contengan conceptos de combustible
            # Patrón: CANTIDAD CLAVE UDM DESCRIPCION OBJ PRECIO IMPORTE IVA
            # Ejemplo: 15459 15101514 LTR GASOLINA REGULAR 02 $18.914799 $292,403.88 $45,375.27
            
            pattern = r'(\d+(?:\.\d+)?)\s+(\d{8})\s+([A-Z]{2,4})\s+(.*?GASOLINA.*?|.*?DIESEL.*?|.*?MAGNA.*?|.*?PREMIUM.*?)\s+(\d{2})\s+\$([0-9,.]+)\s+\$([0-9,.]+)\s+\$([0-9,.]+)'
            
            for match in re.finditer(pattern, page_text, re.I):
                cantidad = _to_dec(match.group(1), prec=4)
                clave_prod_serv = match.group(2)
                clave_unidad = match.group(3).upper()
                if clave_unidad in ("L", "LT"):
                    clave_unidad = "LTR"
                
                descripcion = match.group(4).strip()
                objeto_imp = match.group(5)
                valor_unitario = _to_dec(match.group(6), prec=6)
                importe = _to_dec(match.group(7), prec=2)
                iva_importe = _to_dec(match.group(8), prec=6)
                
                # Calcular base e IVA si es necesario
                base = importe if importe > 0 else (cantidad * valor_unitario)
                tasa = Decimal('0.160000')  # IVA 16% estándar
                
                if iva_importe == 0 and base > 0:
                    iva_importe = (base * tasa).quantize(Decimal('1.000000'))
                
                cpts.append({
                    "Cantidad": cantidad,
                    "ClaveProdServ": clave_prod_serv,
                    "ClaveUnidad": clave_unidad,
                    "Descripcion": descripcion,
                    "ValorUnitario": valor_unitario,
                    "Importe": importe,
                    "NoIdentificacion": "",
                    "ObjetoImp": objeto_imp,
                    "Impuesto": "IVA",
                    "TasaOCuota": tasa,
                    "TipoFactor": "Tasa",
                    "Base": base.quantize(Decimal('1.000000')),
                    "Unidad": clave_unidad,
                    "ImporteImpuesto": iva_importe
                })
    
    return cpts
def extraer_conceptos_enerey(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Extrae conceptos de facturas ENEREY usando regex del texto plano.

    Formato típico en ENEREY:
    21726.00 LITRO  GASPREG  GASOLINA REGULAR  $18.354450  $398,768.78
    """
    cpts: List[Dict[str, Any]] = []

    try:
        import pdfplumber
    except Exception:
        return cpts

    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""

            # Buscar la tabla de conceptos
            # Patrón: CANTIDAD UNIDAD CLAVE DESCRIPCION VALOR_UNITARIO IMPORTE
            # Ejemplo: 21726.00 LITRO GASPREG GASOLINA REGULAR $18.354450 $398,768.78

            pattern = r'(\d+(?:\.\d+)?)\s+(LITRO|LTR)\s+([A-Z]+)\s+(.*?(?:GASOLINA|DIESEL|MAGNA|PREMIUM).*?)\s+\$([0-9,.]+)\s+\$([0-9,.]+)'

            for match in re.finditer(pattern, page_text, re.I):
                cantidad = _to_dec(match.group(1), prec=4)
                unidad_raw = match.group(2).upper()
                clave_unidad = "LTR" if unidad_raw in ("LITRO", "LTR", "L") else unidad_raw

                clave_interna = match.group(3)  # GASPREG, etc.
                descripcion = match.group(4).strip()
                valor_unitario = _to_dec(match.group(5), prec=6)
                importe = _to_dec(match.group(6), prec=2)

                # Buscar clave SAT en el texto (suele estar en líneas siguientes)
                # Ejemplo: "Clave SAT: 15101514 - Gasolina regular menor a 91 octanos"
                clave_prod_serv = "15101515"  # Default
                m_clave = re.search(r'Clave\s+SAT:\s*(\d{8})', page_text, re.I)
                if m_clave:
                    clave_prod_serv = m_clave.group(1)

                # Calcular base e IVA
                base = importe if importe > 0 else (cantidad * valor_unitario)
                tasa = Decimal('0.160000')  # IVA 16% estándar

                # Buscar IVA en el texto
                iva_importe = Decimal('0')
                m_iva = re.search(r'IVA:\s*\$?\s*([0-9,]+\.\d+)', page_text, re.I)
                if m_iva:
                    iva_importe = _to_dec(m_iva.group(1), prec=6)
                else:
                    iva_importe = (base * tasa).quantize(Decimal('1.000000'))

                cpts.append({
                    "Cantidad": cantidad,
                    "ClaveProdServ": clave_prod_serv,
                    "ClaveUnidad": clave_unidad,
                    "Descripcion": descripcion,
                    "ValorUnitario": valor_unitario,
                    "Importe": importe,
                    "NoIdentificacion": clave_interna,  # GASPREG, etc.
                    "ObjetoImp": "02",
                    "Impuesto": "IVA",
                    "TasaOCuota": tasa,
                    "TipoFactor": "Tasa",
                    "Base": base.quantize(Decimal('1.000000')),
                    "Unidad": clave_unidad,
                    "ImporteImpuesto": iva_importe
                })

    return cpts


# conceptos.py

def extraer_conceptos_essafuel(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Extrae conceptos de facturas ESSAFUEL usando regex del texto plano.
    
    Formato típico en ESSAFUEL:
    ClaveProd  Cantidad Unidad No. Identificación  Descripción  Valor Unitario Impuestos  Importe
    15101514   29,838.5500 LTR  H/23183/COM/2020   87 Octanos   $18.968134 002 IVA $43,918.38 $565,981.61
    """
    cpts: List[Dict[str, Any]] = []
    
    try:
        import pdfplumber
    except Exception:
        return cpts
    
    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            
            # Buscar líneas de conceptos
            # Patrón mejorado para capturar conceptos con más flexibilidad
            # ClaveProd Cantidad Unidad No.Ident Descripción Valor$ 002 IVA $IVA $Importe
            
            # Patrón 1: Con todas las columnas presentes
            pattern1 = r'(\d{8})\s+([0-9,]+\.\d+)\s+(LTR|ACT|H87|E48)\s+([A-Z0-9/]+)\s+(.+?)\s+\$([0-9,]+\.\d+)\s+002\s+IVA\s+\$([0-9,]+\.\d+)\s+\$([0-9,]+\.\d+)'
            
            for match in re.finditer(pattern1, page_text):
                clave_prod_serv = match.group(1)
                cantidad = _to_dec(match.group(2), prec=4)
                clave_unidad = match.group(3).upper()
                no_identificacion = match.group(4)
                descripcion = match.group(5).strip()
                valor_unitario = _to_dec(match.group(6), prec=6)
                iva_importe = _to_dec(match.group(7), prec=6)
                importe = _to_dec(match.group(8), prec=2)
                
                # Normalizar unidad
                if clave_unidad in ("L", "LT"):
                    clave_unidad = "LTR"
                
                # Calcular base e IVA si es necesario
                base = importe
                tasa = Decimal('0.160000')  # IVA 16% estándar
                
                # Si el IVA parece ser 8%, ajustar
                if iva_importe > 0 and base > 0:
                    tasa_calculada = (iva_importe / base).quantize(Decimal('0.000001'))
                    if Decimal('0.070000') < tasa_calculada < Decimal('0.090000'):
                        tasa = Decimal('0.080000')
                
                # Validar que IVA calculado coincida (aproximadamente)
                iva_calculado = (base * tasa).quantize(Decimal('1.00'))
                diferencia = abs(iva_importe - iva_calculado)
                
                if diferencia > Decimal('1.00'):
                    # Si hay diferencia significativa, usar el IVA del PDF
                    pass
                
                cpts.append({
                    "Cantidad": cantidad,
                    "ClaveProdServ": clave_prod_serv,
                    "ClaveUnidad": clave_unidad,
                    "Descripcion": descripcion,
                    "ValorUnitario": valor_unitario,
                    "Importe": importe,
                    "NoIdentificacion": no_identificacion,
                    "ObjetoImp": "02",
                    "Impuesto": "IVA",
                    "TasaOCuota": tasa,
                    "TipoFactor": "Tasa",
                    "Base": base.quantize(Decimal('1.000000')),
                    "Unidad": clave_unidad,
                    "ImporteImpuesto": iva_importe.quantize(Decimal('1.000000'))
                })
            
            # Patrón 2: Tabla más simple (sin el formato detallado de IVA)
            # Para casos donde el texto viene en formato diferente
            if not cpts:
                pattern2 = r'(\d{8})\s+([0-9,]+\.\d+)\s+(LTR|ACT)\s+([A-Z0-9/]+)\s+(.+?)(?:\s+\$([0-9,]+\.\d+))?\s+\$([0-9,]+\.\d+)'
                
                for match in re.finditer(pattern2, page_text):
                    clave_prod_serv = match.group(1)
                    cantidad = _to_dec(match.group(2), prec=4)
                    clave_unidad = match.group(3).upper()
                    no_identificacion = match.group(4)
                    descripcion = match.group(5).strip()
                    valor_unitario = _to_dec(match.group(6) or "0", prec=6)
                    importe = _to_dec(match.group(7), prec=2)
                    
                    # Normalizar unidad
                    if clave_unidad in ("L", "LT"):
                        clave_unidad = "LTR"
                    
                    # Si no hay valor unitario, calcularlo
                    if valor_unitario == 0 and cantidad > 0 and importe > 0:
                        valor_unitario = (importe / cantidad).quantize(Decimal('1.000000'))
                    
                    # Calcular base e IVA
                    base = importe
                    tasa = Decimal('0.160000')
                    iva_importe = (base * tasa).quantize(Decimal('1.000000'))
                    
                    cpts.append({
                        "Cantidad": cantidad,
                        "ClaveProdServ": clave_prod_serv,
                        "ClaveUnidad": clave_unidad,
                        "Descripcion": descripcion,
                        "ValorUnitario": valor_unitario,
                        "Importe": importe,
                        "NoIdentificacion": no_identificacion,
                        "ObjetoImp": "02",
                        "Impuesto": "IVA",
                        "TasaOCuota": tasa,
                        "TipoFactor": "Tasa",
                        "Base": base.quantize(Decimal('1.000000')),
                        "Unidad": clave_unidad,
                        "ImporteImpuesto": iva_importe
                    })
    
    return cpts



def extraer_conceptos_premiergas(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Extrae conceptos de facturas PREMIERGAS usando regex del texto plano.
    
    Formato típico en PREMIERGAS:
    CODIGO      COD.SAT  DESCRIPCION              UNIDAD  SERIE  CANT.     PRECIO  %IVA  IVA        IMPORTE
    H_23439_COM_2
    020-34543   15101514 PEMEX MAGNA              LTR            21,019.00 17.81   16.00 59,879.29  386,222.08
    
    NOTA: El CODIGO aparece en DOS líneas y la columna SERIE puede estar vacía.
    """
    cpts: List[Dict[str, Any]] = []
    
    try:
        import pdfplumber
    except Exception:
        return cpts
    
    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            
            # Patrón mejorado que captura la línea de datos SIN el código completo
            # Formato: COD.SAT DESCRIPCION UNIDAD CANT. PRECIO %IVA IVA IMPORTE
            # La columna SERIE está vacía, por eso va directo de UNIDAD a CANT.
            # El CODIGO está en líneas separadas (1-2 líneas arriba)
            pattern = r'(\d{8})\s+((?:PEMEX\s+|DIESEL\s+|GASOLINA\s+)[A-Z\s]+?)\s+(LTR|ACT)\s+([0-9,]+\.\d+)\s+([0-9,]+\.\d+)\s+([0-9.]+)\s+([0-9,]+\.\d+)\s+([0-9,]+\.\d+)'
            
            for match in re.finditer(pattern, page_text, re.I):
                clave_prod_serv = match.group(1)  # 15101514
                descripcion = match.group(2).strip()  # PEMEX MAGNA o DIESEL AUTOMOTRIZ SIN MARCA
                clave_unidad = match.group(3).upper()  # LTR
                cantidad = _to_dec(match.group(4), prec=4)  # 21,019.00
                valor_unitario = _to_dec(match.group(5), prec=6)  # 17.81
                porcentaje_iva = _to_dec(match.group(6))  # 16.00
                iva_importe = _to_dec(match.group(7), prec=6)  # 59,879.29
                importe = _to_dec(match.group(8), prec=2)  # 386,222.08
                
                # Buscar el código interno en las líneas ANTERIORES al match
                # Normalmente está 1-3 líneas arriba
                codigo_interno = ""
                text_before_match = page_text[:match.start()]
                lines_before = text_before_match.split('\n')
                # Buscar hacia atrás patrones como "H_23439_COM_2" o "020-34543"
                for line in reversed(lines_before[-5:]):  # revisar últimas 5 líneas
                    line_stripped = line.strip()
                    # Buscar código que empieza con H_ o tiene guiones/números
                    if re.search(r'^([A-Z0-9_/\-]+)$', line_stripped):
                        if len(line_stripped) >= 3:  # Evitar capturar cosas muy cortas
                            if codigo_interno:
                                # Ya tenemos un código, este es el complementario
                                codigo_interno = f"{line_stripped}/{codigo_interno}"
                                break
                            else:
                                codigo_interno = line_stripped
                
                # Normalizar unidad
                if clave_unidad in ("L", "LT"):
                    clave_unidad = "LTR"
                
                # Calcular base e IVA
                base = importe
                tasa = (porcentaje_iva / Decimal('100')).quantize(Decimal('0.000000'))
                
                cpts.append({
                    "Cantidad": cantidad,
                    "ClaveProdServ": clave_prod_serv,
                    "ClaveUnidad": clave_unidad,
                    "Descripcion": descripcion,
                    "ValorUnitario": valor_unitario,
                    "Importe": importe,
                    "NoIdentificacion": codigo_interno or "",
                    "ObjetoImp": "02",
                    "Impuesto": "IVA",
                    "TasaOCuota": tasa,
                    "TipoFactor": "Tasa",
                    "Base": base.quantize(Decimal('1.000000')),
                    "Unidad": clave_unidad,
                    "ImporteImpuesto": iva_importe.quantize(Decimal('1.000000'))
                })
    
    return cpts


def extraer_conceptos_petrotal(path_pdf: Path) -> List[Dict[str, Any]]:
    """
    Extrae conceptos de facturas PETROTAL usando regex del texto plano.
    
    Formato típico en PETROTAL:
    CANT. CLAVE UDM CONCEPTO REMISIÓN OBJ. PREC. UNIT. IMPORTE IMPTOS.
    29416 15101514 LTR MAXIMA H/22730/COM/2019-F-PET28517-1 02 $18.441144 $542,464.69 $84,112.58
    """
    cpts: List[Dict[str, Any]] = []
    
    try:
        import pdfplumber
    except Exception:
        return cpts
    
    with pdfplumber.open(str(path_pdf)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            
            # Patrón para capturar la línea completa del concepto
            # CANT CLAVE UDM CONCEPTO REMISION OBJ PRECIO IMPORTE IVA
            pattern = r'(\d+(?:\.\d+)?)\s+(\d{8})\s+(LTR|ACT)\s+([A-Z]+)\s+([A-Z0-9/\-]+)\s+(\d{2})\s+\$([0-9,.]+)\s+\$([0-9,.]+)\s+\$([0-9,.]+)'
            
            for match in re.finditer(pattern, page_text, re.I):
                cantidad = _to_dec(match.group(1), prec=4)
                clave_prod_serv = match.group(2)
                clave_unidad = match.group(3).upper()
                descripcion = match.group(4).strip()  # MAXIMA, PREMIUM, etc.
                remision = match.group(5).strip()
                objeto_imp = match.group(6)
                valor_unitario = _to_dec(match.group(7), prec=6)
                importe = _to_dec(match.group(8), prec=2)
                iva_importe = _to_dec(match.group(9), prec=6)
                
                # Calcular base e IVA
                base = importe
                tasa = Decimal('0.160000')  # IVA 16% estándar
                
                # Validar IVA
                if iva_importe == 0 and base > 0:
                    iva_importe = (base * tasa).quantize(Decimal('1.000000'))
                
                cpts.append({
                    "Cantidad": cantidad,
                    "ClaveProdServ": clave_prod_serv,
                    "ClaveUnidad": clave_unidad,
                    "Descripcion": descripcion,
                    "ValorUnitario": valor_unitario,
                    "Importe": importe,
                    "NoIdentificacion": remision,
                    "ObjetoImp": objeto_imp,
                    "Impuesto": "IVA",
                    "TasaOCuota": tasa,
                    "TipoFactor": "Tasa",
                    "Base": base.quantize(Decimal('1.000000')),
                    "Unidad": clave_unidad,
                    "ImporteImpuesto": iva_importe.quantize(Decimal('1.000000'))
                })
    
    return cpts

