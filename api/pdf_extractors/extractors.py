# extractors.py
"""
Extractores de datos específicos por proveedor
"""
import re
import fitz  # PyMuPDF
import pdfplumber
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any, List, Tuple

from .utils import (
    _strip_diacritics, _solo_numeros_forma_pago, _solo_siglas_metodo_pago,
    _moneda_3c, _exportacion_code, _tipo_comprobante_code, dec_from_money,
    parse_iso_datetime, _to_dec, _clean
)
from .profiles import PROFILE_LOBO, PROFILE_MCG, PROFILE_TESORO, PROFILE_AEMSA, PROFILE_ENEREY


def page_text_blocks(page: fitz.Page) -> List[Tuple[float, float, float, float, str]]:
    """Devuelve lista de bloques: (x0, y0, x1, y1, text)"""
    out = []
    for b in page.get_text("blocks"):
        if len(b) >= 5:
            x0, y0, x1, y1, text = b[:5]
            out.append((x0, y0, x1, y1, text or ""))
    return out


def read_near_anchor(page: fitz.Page, anchor_text: str, rect_offset=(0,0,250,30)) -> str:
    """
    Busca un bloque que contenga anchor_text y lee texto en un rectángulo adyacente.
    """
    at_up = _strip_diacritics(anchor_text).upper()
    for (x0, y0, x1, y1, text) in page_text_blocks(page):
        tnorm = _strip_diacritics(text).upper()
        if at_up in tnorm:
            dx_r, dy_t, dx_w, dy_h = rect_offset

            # Busca derecha
            rect_right = fitz.Rect(x1, y0 + dy_t, x1 + dx_w, y0 + dy_h)
            s = page.get_textbox(rect_right) or ""
            if s.strip():
                return s.strip()

            # Busca debajo
            rect_below = fitz.Rect(x0, y1 + dy_t, x0 + max(dx_w, (x1 - x0) + 250), y1 + dy_h + 25)
            s = page.get_textbox(rect_below) or ""
            if s.strip():
                return s.strip()

            # Busca derecha-inferior
            rect_rb = fitz.Rect(x1, y1 + dy_t, x1 + dx_w, y1 + dy_h)
            s = page.get_textbox(rect_rb) or ""
            if s.strip():
                return s.strip()

    return ""


def extract_with_profile_lobo(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos para FacturasRecibidas usando el perfil LOBO."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "", "Remision": ""  # ← NUEVO CAMPO
    }

    # Texto global para regex de respaldo
    full_text_pages = [p.get_text() or "" for p in doc]
    full_text = _strip_diacritics(" ".join(full_text_pages))

    # Emisor/Receptor por regex global
    regex_patterns = PROFILE_LOBO["regex"]
    
    for key, pattern in regex_patterns.items():
        m = re.search(pattern, full_text, re.I|re.DOTALL)
        if m:
            if key == "emisor_nombre":
                data["EmisorNombre"] = m.group(1).strip()
            elif key == "emisor_rfc":
                data["EmisorRFC"] = m.group(1).strip()
            elif key == "emisor_regimen":
                data["EmisorRegimenFiscal"] = m.group(1).strip()
            elif key == "receptor_nombre":
                data["ReceptorNombre"] = m.group(1).strip()
            elif key == "receptor_rfc":
                data["ReceptorRfc"] = m.group(1).strip()

    # Campos de cabecera por ancla-rectángulo
    anchors = PROFILE_LOBO["anchors"]

    # --- dentro de extract_with_profile_lobo ---
    for p in doc:
        page_text = _strip_diacritics(p.get_text() or "")

        for field, anchor_config in anchors.items():

            # Decidir si este campo "falta" según el mapeo correcto (field en minúsculas)
            def needs(f: str) -> bool:
                return {
                    "uuid":             lambda: not data["UUID"],
                    "folio":            lambda: not data["Folio"],
                    "fecha_timbrado":   lambda: data["FechaTimbrado"] is None,
                    "fecha_emision":    lambda: data["Fecha"] is None,
                    "lugar_expedicion": lambda: not data["LugarExpedicion"],
                    "tipo_comprobante": lambda: not data["TipoDeComprobante"],
                    "forma_pago":       lambda: not data["FormaPago"],
                    "metodo_pago":      lambda: not data["MetodoPago"],
                    "moneda":           lambda: not data["Moneda"],
                    "exportacion":      lambda: not data["Exportacion"],
                    "subtotal":         lambda: data["SubTotal"] == 0,
                    "total":            lambda: data["Total"] == 0,
                }.get(f, lambda: True)()

            if not needs(field):
                continue

            # ---- a partir de aquí deja tus mismos bloques por tipo de campo ----
            if field in ["fecha_timbrado", "fecha_emision"]:
                snippet = read_near_anchor(p, anchor_config["anchor_text"], anchor_config["rect_offset"])
                m = re.search(anchor_config["regex"], _strip_diacritics(snippet), re.I)
                if m:
                    parsed_date = parse_iso_datetime(m.group(0))
                    if field == "fecha_timbrado":
                        data["FechaTimbrado"] = parsed_date
                    else:
                        data["Fecha"] = parsed_date

            elif field == "folio":
               # 1) Regex directo en el texto de página - mejorado para capturar números después de "Factura"
                m = re.search(r'\bFactura\s+(\d+)', page_text, re.I)
                if m:
                    data["Folio"] = m.group(1)
                else:
                    # 2) Intenta con otros formatos posibles
                    m = re.search(r'\bFactura\s*[:#]\s*([A-Za-z0-9-]+)', page_text, re.I)
                    if m:
                        data["Folio"] = m.group(1)
                    else:
                        # 3) Respaldo por vecindad al ancla
                        snippet = read_near_anchor(p, anchor_config["anchor_text"], anchor_config["rect_offset"])
                        m = re.search(r'([A-Za-z0-9-]+)', _strip_diacritics(snippet), re.I)
                        if m:
                            data["Folio"] = m.group(1)
            elif field in ["uuid", "lugar_expedicion"]:
                snippet = read_near_anchor(p, anchor_config["anchor_text"], anchor_config["rect_offset"])
                m = re.search(anchor_config["regex"], _strip_diacritics(snippet), re.I)
                if m:
                    if field == "uuid":
                        data["UUID"] = m.group(0)
                    else:
                        data["LugarExpedicion"] = m.group(0)

            elif field in ["tipo_comprobante", "forma_pago", "metodo_pago", "moneda", "exportacion"]:
                m = re.search(anchor_config["regex"], page_text, re.I)
                if m:
                    if field == "tipo_comprobante":
                        data["TipoDeComprobante"] = m.group(1).upper()
                    elif field == "forma_pago":
                        data["FormaPago"] = m.group(1).strip()
                    elif field == "metodo_pago":
                        data["MetodoPago"] = m.group(1).strip()
                    elif field == "moneda":
                        data["Moneda"] = m.group(1).strip()
                    elif field == "exportacion":
                        data["Exportacion"] = m.group(1).strip()

            elif field in ["subtotal", "total"]:
                m = re.search(anchor_config["regex"], page_text, re.I)
                if m:
                    val = dec_from_money(m.group(1))
                    if field == "subtotal":
                        data["SubTotal"] = val
                    else:
                        data["Total"] = val

    m_destino = re.search(
        r'DESTINO\s+([A-Z0-9\s]+?)(?:\s+PIPA|\s+PERMISO|$)',
        full_text,
        re.I
    )
    if m_destino:
        data["Destino"] = m_destino.group(1).strip()
    else:
        # Búsqueda alternativa más flexible
        m_destino = re.search(
            r'ETIQUETAS\s+PERSONALIZADAS[^\n]*\n[^\n]*DESTINO\s+([^\n]+)',
            full_text,
            re.I | re.DOTALL
        )
        if m_destino:
            # Limpiar el texto capturado
            destino_raw = m_destino.group(1).strip()
            # Eliminar texto después de PIPA, SELLOS, PERMISO
            destino_clean = re.split(r'\s+(?:PIPA|SELLOS|PERMISO)', destino_raw, maxsplit=1)[0]
            data["Destino"] = destino_clean.strip()
    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c(data["Moneda"])
    data["Exportacion"] = _exportacion_code(data["Exportacion"])
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data



def extract_with_profile_mcg(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil MCG."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",   # ← NUEVO CAMPO
        "Remision": ""   # ← PARA CONSISTENCIA (vacío en MCG)
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === EMISOR/RECEPTOR MEJORADO ===
    
    # Emisor nombre - buscar específicamente MGC MEXICO sin contaminación
    m = re.search(r'R\.F\.C\.\s*(MME\d+[A-Z0-9]+)\s+\d+\s+([^0-9]+?)(?:Juarez|JUAREZ)', full_text, re.I)
    if not m:
        m = re.search(r'\b(MGC\s*MEXICO)\b', full_text, re.I)
    if m:
        if len(m.groups()) > 1:
            data["EmisorRFC"] = m.group(1).strip()
            data["EmisorNombre"] = "MGC MEXICO"  # Hardcoded porque siempre es este
        else:
            data["EmisorNombre"] = m.group(1).strip()
    
    # Emisor RFC
    if not data["EmisorRFC"]:
        m = re.search(r'\b(MME141110IJ9)\b', full_text, re.I)
        if m:
            data["EmisorRFC"] = m.group(1).strip()
    
    # Emisor Régimen Fiscal
    m = re.search(r'Regimen\s*Fiscal\s+(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()
    
    # Receptor - patrón específico de MCG
    m = re.search(r'FACTURADO\s+A\s*\n?\s*(?:FECHA[^\n]*\n)?\s*([A-Z0-9\s]+?)\s+([A-Z]{3}\d{6}[A-Z0-9]{3})', full_text, re.I|re.DOTALL)
    if m:
        data["ReceptorNombre"] = m.group(1).strip()
        data["ReceptorRfc"] = m.group(2).strip()
    else:
        # Intento alternativo más directo
        m = re.search(r'([A-Z]+(?:\s+[A-Z]+)*)\s+([A-Z]{3}\d{6}[A-Z0-9]{3})', full_text)
        if m and "FACTURADO" in full_text[:full_text.find(m.group(0))]:
            data["ReceptorNombre"] = m.group(1).strip()
            data["ReceptorRfc"] = m.group(2).strip()

    # === FOLIO - buscar CO ######## ===
    m = re.search(
        r'Cadena\s+original[^\|]*\|\|4\.0\|([A-Z0-9]{2,8})\|(\d{3,})\|', 
        full_text, re.I
    )
    if m:
        serie_cad, folio_cad = m.group(1), m.group(2)
        data["Folio"] = folio_cad  # tu tabla usa Folio; Serie guárdala sólo si la necesitas
        data["__Serie"] = serie_cad  # opcional, no afecta tu INSERT actual

    # Fallback: cerca de “Folio Fiscal … SERIE ######”
    if not data.get("Folio"):
        m = re.search(
            r'Folio\s+Fiscal[^\n]*\b([A-Z]{2,8})\s+(\d{3,})\b',
            full_text, re.I
        )
        if m:
            data["Folio"] = m.group(2)
            data["__Serie"] = m.group(1)

    # (Mantén tu regex antiguo de "CO \d+" por compatibilidad con otras impresiones)
    if not data.get("Folio"):
        m = re.search(r'\b([A-Z]{2,8})\s+(\d{3,})\b', full_text)
        if m and "Folio Fiscal" in full_text[: full_text.find(m.group(0))]:
            data["Folio"] = m.group(2)
            data["__Serie"] = m.group(1)
    
    # === UUID - VERSIÓN MEJORADA Y ROBUSTA ===
    # Método 1: Buscar UUID completo en una sola línea
    m = re.search(r'([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).upper()
    else:
        # Método 2: Buscar UUID dividido en múltiples líneas cerca de "Folio Fiscal"
        # Primero buscar la zona alrededor de "Folio Fiscal"
        folio_match = re.search(r'Folio\s*Fiscal[^\n]*\n([^\n]*)\n?([^\n]*)', full_text, re.I|re.DOTALL)
        if folio_match:
            # Combinar las líneas después de "Folio Fiscal"
            uuid_area = (folio_match.group(1) + " " + (folio_match.group(2) or "")).replace('-', '').replace(' ', '')
            # Buscar 32 caracteres hexadecimales consecutivos
            m = re.search(r'([A-F0-9]{32})', uuid_area, re.I)
            if m:
                hex_str = m.group(1)
                # Formatear como UUID: 8-4-4-4-12
                formatted_uuid = f"{hex_str[0:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"
                data["UUID"] = formatted_uuid.upper()
        
        # Método 3: Buscar UUID fragmentado con guiones en cualquier lugar del texto
        if not data["UUID"]:
            # Buscar partes del UUID por separado y reconstruir
            parts = re.findall(r'([A-F0-9]{8,12})', full_text, re.I)
            if len(parts) >= 5:
                # Intentar encontrar las partes que sumen 32 caracteres
                for i in range(len(parts) - 4):
                    candidate = ''.join(parts[i:i+5])
                    if len(candidate) == 32:
                        formatted_uuid = f"{candidate[0:8]}-{candidate[8:12]}-{candidate[12:16]}-{candidate[16:20]}-{candidate[20:32]}"
                        data["UUID"] = formatted_uuid.upper()
                        break
        
        # Método 4: Búsqueda específica para el patrón de MCG (como último recurso)
        if not data["UUID"]:
            # Buscar el patrón específico que aparece en los PDFs de MCG
            m = re.search(r'([A-F0-9]{8})-([A-F0-9]{4})-([A-F0-9]{4})-([A-F0-9]{4})-?\s*\n?\s*([A-F0-9]{12})', full_text, re.I)
            if m:
                data["UUID"] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}-{m.group(5)}"
            else:
                # Último intento: buscar patrón dividido en líneas
                uuid_pattern = r'([A-F0-9]{8})[^A-F0-9]*([A-F0-9]{4})[^A-F0-9]*([A-F0-9]{4})[^A-F0-9]*([A-F0-9]{4})[^A-F0-9]*([A-F0-9]{12})'
                m = re.search(uuid_pattern, full_text, re.I)
                if m:
                    data["UUID"] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}-{m.group(5)}"
    
    # === FECHA EMISIÓN ===
    m = re.search(r'Fecha\s+Factura:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        data["Fecha"] = parse_iso_datetime(m.group(1))
    
    # === FECHA TIMBRADO ===
    m = re.search(r'Fecha\s+y\s+hora\s+de\s+Certificacion:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        data["FechaTimbrado"] = parse_iso_datetime(m.group(1))
    
    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'Lugar\s+de\s+expedicion\s+(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()
    
    # === TIPO COMPROBANTE ===
    m = re.search(r'Tipo\s+de\s+Comprobante[:\s]+([IEPNT])', full_text, re.I)
    if m:
        data["TipoDeComprobante"] = m.group(1).upper()

    # --- Complemento de pago: marcar bandera si aplica ---
    # Cualquiera de estas señales sirve:
    # - Tipo 'P' (Pago)
    # - "Uso del CFDI: CP01" (Pagos)
    # - Frase "Informacion de recepcion de pagos" (versión 2.0)
    # - En muchos MCG, "Moneda XXX" (sin moneda de transacción)
    is_pago = False
    if (data.get("TipoDeComprobante", "").upper() == "P" or
        re.search(r'Uso\s+del\s+CFDI\s*:\s*CP01', full_text, re.I) or
        re.search(r'Informacion\s+de\s+recepcion\s+de\s+pagos', full_text, re.I) or
        re.search(r'\bMoneda\s+XXX\b', full_text, re.I)):
        is_pago = True

    data["__is_pago"] = is_pago
    
    # === FORMA DE PAGO - corregido ===
    m = re.search(r'Forma\s+de\s+pago\s+(?:Lugar[^\n]*\n)?(\d{2})', full_text, re.I|re.DOTALL)
    if m:
        data["FormaPago"] = m.group(1).strip()
    
    # === MÉTODO DE PAGO - corregido ===
    m = re.search(r'Metodo\s+de\s+pago\s+(?:Forma[^\n]*\n)?([A-Z]{3})', full_text, re.I|re.DOTALL)
    if m:
        data["MetodoPago"] = m.group(1).strip()
    
    # === MONEDA ===
    m = re.search(r'Total\s+[0-9,]+\.\d+\s+[A-Z]+\s+[0-9,]+\.\d+\s+([A-Z]{3})', full_text, re.I)
    if m:
        data["Moneda"] = m.group(1).strip()
    
    # === SUBTOTAL ===
    m = re.search(r'Subtotal\s+([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))
    
    # === TOTAL - buscar el total real, no el subtotal ===
    # Buscar patrón: Total ###.## L ###,###.## MXN
    m = re.search(r'Total\s+[0-9,]+\.\d+\s+[A-Z]+\s+([0-9,]+\.\d+)\s+[A-Z]{3}', full_text, re.I)
    if m:
        data["Total"] = dec_from_money(m.group(1))
    elif data["SubTotal"] > 0:
        # Si no encontramos el total explícito, buscar después de la palabra en mayúsculas entre paréntesis
        m = re.search(r'\([A-Z\s]+PESOS[^)]*\)[^\d]*([0-9,]+\.\d+)', full_text, re.I)
        if m:
            data["Total"] = dec_from_money(m.group(1))
    
    m_destino = re.search(
        r'PRODUCTO\s+DESTINO\s+FECHA[^\n]*\n[^\n]*?\s+((?:E\d{5})|(?:[A-Z]{1,3}/[\d]+/[A-Z]+/[A-Z]+/\d{4}_?))\s',
        full_text,
        re.I
    )
    if m_destino:
        data["Destino"] = m_destino.group(1).strip()
    else:
        # Patrón 2: Buscar código E##### (estaciones)
        m_destino = re.search(r'\b(E\d{5})\b', full_text)
        if m_destino:
            data["Destino"] = m_destino.group(1).strip()
        else:
            # Patrón 3: Buscar permisos CRE (PL/####/EXP/ES/####)
            m_destino = re.search(r'\b([A-Z]{1,3}/\d+/[A-Z]+/[A-Z]+/\d{4}_?)\b', full_text)
            if m_destino:
                data["Destino"] = m_destino.group(1).strip()
    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c(data["Moneda"])
    data["Exportacion"] = _exportacion_code("01")  # MCG usa 01 (No aplica)
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data


def extract_with_profile_aemsa(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil AEMSA."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",
        "Remision": ""
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === EMISOR ===
    m = re.search(r'(ALTOS\s+ENERGETICOS\s+MEXICANOS)', full_text, re.I)
    if m:
        data["EmisorNombre"] = m.group(1).strip()

    # RFC Emisor: buscar el patrón "RFC: AEM-160511-LMA" o "RFC: AEM160511LMA"
    m = re.search(r'RFC:\s*(AEM[-]?\d{6}[-]?[A-Z0-9]{3})', full_text, re.I)
    if m:
        rfc_raw = m.group(1).strip()
        # Remover guiones para normalizar
        data["EmisorRFC"] = rfc_raw.replace('-', '')

    m = re.search(r'REGIMEN:\s*(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()

    # === RECEPTOR RFC ===
    # El RFC aparece en una línea separada después de "FACTURADO A RFC:"
    # Ejemplo:
    # FACTURADO A RFC:
    # SERVICIOS GASOLINEROS EL CASTAÑO SA DE CV
    # SGC-130412-9H9
    
    # Intentar buscar en línea separada (más común)
    m = re.search(r'FACTURADO\s+[Aa]\s+RFC:\s*\n[^\n]*\n\s*([A-Z]{3}[-]?\d{6}[-]?[A-Z0-9]{3})', full_text, re.I)
    if m:
        rfc_receptor = m.group(1).strip()
        # Eliminar guiones como solicita el usuario
        data["ReceptorRfc"] = rfc_receptor.replace('-', '')
    else:
        # Alternativa: RFC en la misma línea (respaldo)
        m = re.search(r'FACTURADO\s+[Aa]\s+RFC:?\s*([A-Z]{3}[-]?\d{6}[-]?[A-Z0-9]{3})', full_text, re.I)
        if m:
            rfc_receptor = m.group(1).strip()
            data["ReceptorRfc"] = rfc_receptor.replace('-', '')

    # Nombre del receptor viene después del RFC (en la siguiente línea)
    m = re.search(r'FACTURADO\s+[Aa]\s+RFC:?\s*[A-Z]{3}[-]?\d{6}[-]?[A-Z0-9]{3}\s+([A-Z\s&.]+?)(?:\s+USO\s+CFDI)', full_text, re.I)
    if m:
        data["ReceptorNombre"] = m.group(1).strip()
    else:
        # Buscar nombre después del RFC (línea separada)
        m = re.search(r'FACTURADO\s+[Aa]\s+RFC:?\s*[^\n]+\n\s*([A-Z][A-Z\s&.]+(?:SA|S\.A\.|DE\s+CV|S\.\s*DE\s*R\.L\.))', full_text, re.I)
        if m:
            data["ReceptorNombre"] = m.group(1).strip()

    # === FOLIO ===
    # En AEMSA, el folio F##### aparece ANTES de la palabra FACTURA
    # Ejemplo: "F89667\nFACTURA"
    m = re.search(r'(F\d+)\s+FACTURA', full_text, re.I)
    if m:
        data["Folio"] = m.group(1).strip()
    else:
        # Patrón alternativo: FACTURA seguido de F##### (por si acaso)
        m = re.search(r'FACTURA\s+(F\d+)', full_text, re.I)
        if m:
            data["Folio"] = m.group(1).strip()

    # === UUID ===
    m = re.search(r'Folio\s+Fiscal:\s*([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).upper()

    # === FECHA EMISIÓN (está debajo de "Fecha del Documento") ===
    # Buscar la fecha que viene después del texto "Fecha del Documento"
    m = re.search(r'Fecha\s+del\s+Documento[^\d]*(\d{2}/\d{2}/\d{4})[^\d]*(\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        # Convertir de dd/mm/yyyy a yyyy-mm-dd
        fecha_str = m.group(1)  # 25/09/2025
        hora_str = m.group(2)   # 10:38:53
        partes = fecha_str.split('/')
        fecha_iso = f"{partes[2]}-{partes[1]}-{partes[0]}T{hora_str}"
        data["Fecha"] = parse_iso_datetime(fecha_iso)
    else:
        # Alternativa: buscar formato de fecha cerca del folio
        m = re.search(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text)
        if m:
            fecha_str = m.group(1)
            hora_str = m.group(2)
            partes = fecha_str.split('/')
            fecha_iso = f"{partes[2]}-{partes[1]}-{partes[0]}T{hora_str}"
            data["Fecha"] = parse_iso_datetime(fecha_iso)

    # === FECHA TIMBRADO ===
    # La fecha aparece ANTES del texto "Fecha y Hora de cert.:"
    # Ejemplo:
    # 19/09/2025 23:28:39
    # Fecha y Hora de cert.:
    
    # Buscar fecha ANTES de "Fecha y Hora de cert.:"
    m = re.search(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s*\n\s*Fecha\s+y\s+Hora\s+de\s+cert\.?:', full_text, re.I)
    if m:
        fecha_str = m.group(1)
        hora_str = m.group(2)
        partes = fecha_str.split('/')
        fecha_iso = f"{partes[2]}-{partes[1]}-{partes[0]}T{hora_str}"
        data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)
    else:
        # Alternativa: buscar simplemente la segunda ocurrencia de fecha/hora (después de Fecha del Documento)
        # Esto captura la fecha de timbrado
        fechas = list(re.finditer(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text))
        if len(fechas) >= 2:
            # La segunda fecha suele ser la de timbrado
            m = fechas[1]
            fecha_str = m.group(1)
            hora_str = m.group(2)
            partes = fecha_str.split('/')
            fecha_iso = f"{partes[2]}-{partes[1]}-{partes[0]}T{hora_str}"
            data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)

    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'Lugar\s+de\s+Expedicion\s+(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()

    # === TIPO COMPROBANTE ===
    m = re.search(r'Tipo\s+Comp\.?\s*:\s*([IEPNT])', full_text, re.I)
    if m:
        data["TipoDeComprobante"] = m.group(1).upper()

    # === FORMA DE PAGO ===
    m = re.search(r'FORMA\s+DE\s+PAGO:\s*([^\r\n]+?)(?=\s*METODO\s+DE\s+PAGO)', full_text, re.I)
    if m:
        data["FormaPago"] = m.group(1).strip()

    # === MÉTODO DE PAGO ===
    m = re.search(r'METODO\s+DE\s+PAGO:\s*([^\r\n]+?)(?=\s*MONEDA)', full_text, re.I)
    if m:
        data["MetodoPago"] = m.group(1).strip()

    # === MONEDA ===
    m = re.search(r'MONEDA:\s*([A-Z]{3})', full_text, re.I)
    if m:
        data["Moneda"] = m.group(1).strip()

    # === SUBTOTAL ===
    m = re.search(r'SUBTOTAL:\s*\$?\s*([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))

    # === TOTAL ===
    # En AEMSA hay dos ocurrencias de TOTAL:
    # 1. En la tabla de conceptos (columna IMPORTE)
    # 2. Al pie de la factura (después del IVA)
    # 
    # El formato es:
    # IVA 16%:
    # $337,779.15
    # TOTAL:
    #
    # Necesitamos capturar el monto que está entre "IVA 16%:" y "TOTAL:"
    
    m = re.search(r'IVA\s+16%:\s*\n\s*\$?\s*([0-9,]+\.\d+)\s*\n\s*TOTAL:', full_text, re.I)
    if m:
        data["Total"] = dec_from_money(m.group(1))
    else:
        # Alternativa: buscar la última ocurrencia de monto antes de "TOTAL:"
        # que venga después de SUBTOTAL
        idx_subtotal = full_text.upper().find('SUBTOTAL:')
        if idx_subtotal >= 0:
            text_after_subtotal = full_text[idx_subtotal:]
            # Buscar todos los montos que vienen antes de "TOTAL:"
            matches = list(re.finditer(r'\$?\s*([0-9,]+\.\d+)\s*\n[^\n]*TOTAL:', text_after_subtotal, re.I))
            if matches:
                # El último monto antes de TOTAL: es el total real
                data["Total"] = dec_from_money(matches[-1].group(1))

    # === DESTINO ===
    # Buscar en "Dirección de Entrega"
    m = re.search(r'Direccion\s+de\s+Entrega\s+([^\n]+?)(?:\s+DIR\d+)', full_text, re.I)
    if m:
        data["Destino"] = m.group(1).strip()
    else:
        # Alternativa: buscar código DIR#####
        m = re.search(r'(DIR\d{7})', full_text, re.I)
        if m:
            data["Destino"] = m.group(1).strip()

    # === REMISIÓN ===
    # Buscar en observaciones o en números de referencia
    m = re.search(r'Observaciones[^\n]*\n[^\n]*?(\d{6,})', full_text, re.I)
    if m:
        data["Remision"] = m.group(1).strip()

    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c(data["Moneda"])
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data


def extract_with_profile_enerey(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil ENEREY - VERSIÓN CORREGIDA."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",
        "Remision": ""
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === DETECTAR FACTURA CANCELADA ===
    if (re.search(r'Acuse\s+de\s+cancelacion', full_text, re.I) or
        re.search(r'Cancelado\s+con\s+aceptacion', full_text, re.I) or
        re.search(r'Estatus\s+UUID:\s*Cancelado', full_text, re.I)):
        data["__is_cancelada"] = True
        return data  # Retornar inmediatamente sin extraer más datos

    # === EMISOR - HARDCODEADO (viene como imagen) ===
    data["EmisorNombre"] = "ENEREY LATINOAMERICA"
    data["EmisorRFC"] = "SGE151215F71"
    
    # Intentar extraer RFC de la cadena original como respaldo (por si cambia)
    m = re.search(r'\b(SGE\d{6}[A-Z0-9]{3})\b', full_text, re.I)
    if m:
        data["EmisorRFC"] = m.group(1).strip()

    # Régimen Fiscal
    m = re.search(r'Regimen\s+fiscal\s+emisor:\s*(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()
    else:
        data["EmisorRegimenFiscal"] = "601"

    # === RECEPTOR RFC (buscar primero porque está antes que el nombre en el texto) ===
    all_rfcs = re.findall(r'\b([A-Z]{3}\d{6}[A-Z0-9]{3})\b', full_text)
    for rfc in all_rfcs:
        if not rfc.startswith('SGE'):
            data["ReceptorRfc"] = rfc
            break

    # === RECEPTOR NOMBRE ===
    if data["ReceptorRfc"]:
        rfc_pos = full_text.find(data["ReceptorRfc"])
        if rfc_pos > 0:
            text_before = full_text[max(0, rfc_pos-200):rfc_pos]
            m = re.findall(r'\b([A-Z][A-Z\s&.]{3,40})\b', text_before)
            if m:
                data["ReceptorNombre"] = m[-1].strip()

    # === FOLIO ===
    m = re.search(r'\b(E)\s+(\d{5})\b', full_text)
    if m:
        data["Folio"] = f"{m.group(1)}{m.group(2)}"
    else:
        m = re.search(r'Factura[^\d]+(E\s*\d+)', full_text, re.I)
        if m:
            data["Folio"] = re.sub(r'\s+', '', m.group(1))

    # === UUID ===
    m = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).lower()

    # === FECHAS - MEJORADO PARA MANEJAR ESPACIOS VARIABLES ===
    # Buscar todas las fechas/horas en el documento con espacios flexibles
    fechas_matches = list(re.finditer(
        r'(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2}:\d{2})',
        full_text
    ))
    
    if len(fechas_matches) >= 2:
        # La segunda fecha es la de emisión
        m = fechas_matches[1]
        dia, mes, anio = m.group(1).split('/')
        hora = m.group(2)
        # Normalizar hora a HH:MM:SS si viene como H:MM:SS
        if len(hora.split(':')[0]) == 1:
            hora = '0' + hora
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["Fecha"] = parse_iso_datetime(fecha_iso)

    if len(fechas_matches) >= 1:
        # La primera fecha es la de certificación/timbrado
        m = fechas_matches[0]
        dia, mes, anio = m.group(1).split('/')
        hora = m.group(2)
        if len(hora.split(':')[0]) == 1:
            hora = '0' + hora
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)

    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'Lugar\s+de\s+expedicion:\s*(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()
    else:
        m = re.search(r'(\d{5})\s*-\s*SAN\s+PEDRO\s+GARZA\s+GARCIA', full_text, re.I)
        if m:
            data["LugarExpedicion"] = m.group(1).strip()

    # === TIPO COMPROBANTE ===
    m = re.search(r'([IEPNT])\s*-\s*(Ingreso|Egreso|Traslado|Nomina|Pago)', full_text, re.I)
    if m:
        data["TipoDeComprobante"] = m.group(1).upper()
    else:
        m = re.search(r'Tipo\s+de\s+Comprobante[:\s]*([IEPNT])', full_text, re.I)
        if m:
            data["TipoDeComprobante"] = m.group(1).upper()

    # === DETECTAR COMPLEMENTO DE PAGO O NOTA DE CRÉDITO ===
    is_pago = False
    is_nota_credito = False
    
    tipo_comp = data.get("TipoDeComprobante", "").upper()
    
    if tipo_comp == "P":
        is_pago = True
    elif tipo_comp == "E":
        is_nota_credito = True
    
    # Verificaciones adicionales para complemento de pago
    if (re.search(r'Uso\s+de\s+CFDI:\s*CP01', full_text, re.I) or
        re.search(r'Recibo\s+de\s+pago', full_text, re.I) or
        re.search(r'MontoTotalPagos=', full_text, re.I)):
        is_pago = True
    
    # Verificaciones adicionales para nota de crédito
    if (re.search(r'Nota\s+de\s+credito', full_text, re.I) or
        re.search(r'G02\s*-\s*Devoluciones', full_text, re.I) or
        re.search(r'Descuento\s+N\d+', full_text, re.I)):
        is_nota_credito = True

    data["__is_pago"] = is_pago
    data["__is_nota_credito"] = is_nota_credito

    # === FORMA DE PAGO ===
    m = re.search(r'Forma\s+de\s+pago:\s*(\d{2})', full_text, re.I)
    if m:
        data["FormaPago"] = m.group(1)
    else:
        m = re.search(r'Forma\s+de\s+pago:\s*([^\n]+)', full_text, re.I)
        if m:
            data["FormaPago"] = m.group(1).strip()

    # === MÉTODO DE PAGO ===
    m = re.search(r'Metodo\s+de\s+pago:\s*([A-Z]{3})', full_text, re.I)
    if m:
        data["MetodoPago"] = m.group(1)
    else:
        m = re.search(r'Metodo\s+de\s+pago:\s*([^\n]+)', full_text, re.I)
        if m:
            data["MetodoPago"] = m.group(1).strip()

    # === SUBTOTAL ===
    m = re.search(r'Subtotal:\s*\$?\s*([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))

    # === TOTAL ===
    m = re.search(r'IVA:.*?TOTAL:\s*\$?\s*([0-9,]+\.\d+)', full_text, re.I | re.DOTALL)
    if m:
        data["Total"] = dec_from_money(m.group(1))
    else:
        m = re.search(r'\bTOTAL:\s*\$?\s*([0-9,]+\.\d+)', full_text)
        if m:
            data["Total"] = dec_from_money(m.group(1))

    # === DESTINO ===
    m = re.search(r'Pedido\s+Interno:\s*([^\n]+)', full_text, re.I)
    if m:
        destino_completo = m.group(1).strip()
        m_permiso = re.search(r'(PL/\d+/[A-Z]+/[A-Z]+/\d{4})', destino_completo)
        if m_permiso:
            data["Destino"] = m_permiso.group(1)
        else:
            data["Destino"] = destino_completo.split()[0] if destino_completo else ""
    else:
        m = re.search(r'Entregado\s+a:\s*([^\n]+)', full_text, re.I)
        if m:
            destino_completo = m.group(1).strip()
            m_permiso = re.search(r'(PL/\d+/[A-Z]+/[A-Z]+/\d{4})', destino_completo)
            if m_permiso:
                data["Destino"] = m_permiso.group(1)
            else:
                data["Destino"] = destino_completo.split()[0] if destino_completo else ""

    # === REMISIÓN ===
    m = re.search(r'REMISION:\s*(\d+)', full_text, re.I)
    if m:
        data["Remision"] = m.group(1).strip()
    else:
        # Alternativa: buscar en observaciones
        m = re.search(r'Remision:\s*(\d+)', full_text, re.I)
        if m:
            data["Remision"] = m.group(1).strip()

    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c("MXN")
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data


def extraer_datos_pdf(path_pdf: Path, provider_hint: str = None) -> Dict[str, Any]:
    """Función principal para extraer datos de PDF según el proveedor detectado."""
    from utils import detect_provider_profile
    
    with fitz.open(str(path_pdf)) as doc:
        # Detección de proveedor
        full_text = " ".join([p.get_text() or "" for p in doc])
        provider_key = detect_provider_profile(full_text, provider_hint=provider_hint)

        if provider_key == "lobo":
            data = extract_with_profile_lobo(doc)
        elif provider_key == "mcg":
            data = extract_with_profile_mcg(doc)
        elif provider_key == "tesoro":
            data = extract_with_profile_tesoro(doc)
        elif provider_key == "aemsa":
            data = extract_with_profile_aemsa(doc)
        elif provider_key == "enerey":
            data = extract_with_profile_enerey(doc)
        elif provider_key == "essafuel":  # ← NUEVO
            data = extract_with_profile_essafuel(doc)
        elif provider_key == "premiergas":  # ← AGREGAR ESTO
            data = extract_with_profile_premiergas(doc)
        elif provider_key == "petrotal":  # ← AGREGAR ESTE CASO
            data = extract_with_profile_petrotal(doc)
        else:
            raise ValueError(f"Proveedor no soportado aún para: {path_pdf.name}")
        
        data["__path"] = str(path_pdf)
        data["__provider"] = provider_key

        # Validaciones mínimas y respaldos
        if not data["UUID"]:
            print(f"  UUID no detectado en {path_pdf.name}. Se intentará regex global adicional.")
            m = re.search(r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}', _strip_diacritics(full_text), re.I)
            if m:
                data["UUID"] = m.group(0)
        
        if data["Fecha"] is None:
            m = re.search(r'FECHA Y HORA DE EMISI[ÓO]N DE CFDI\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
            if m:
                data["Fecha"] = parse_iso_datetime(m.group(1))

        if data["FechaTimbrado"] is None:
            m = re.search(r'FECHA Y HORA DE CERTIFICACI[ÓO]N\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
            if m:
                data["FechaTimbrado"] = parse_iso_datetime(m.group(1))

        return data



def extract_with_profile_tesoro(doc: fitz.Document) -> Dict[str, Any]:
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",    # ← NUEVO
        "Remision": ""    # ← NUEVO
    }
    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # Emisor/Receptor
    m = re.search(r'(TESORO\s+MEXICO\s+SUPPLY\s*&\s*MARKETING)', full_text, re.I)
    if m: data["EmisorNombre"] = m.group(1).strip()
    m = re.search(r'\b(TMS1611162N5)\b', full_text, re.I)
    if m: data["EmisorRFC"] = m.group(1).strip()
    m = re.search(r'REGIMEN\s+FISCAL:\s*(\d{3})', full_text, re.I)
    if m: data["EmisorRegimenFiscal"] = m.group(1).strip()

    m = re.search(r'PARA:\s*([\w\s&.]+)\s+([A-Z]{3}\d{6}[A-Z0-9]{3})', full_text, re.I)
    if m:
        data["ReceptorNombre"] = m.group(1).strip()
        data["ReceptorRfc"] = m.group(2).strip()

    # UUID (puede venir partido entre líneas)
    m = re.search(r'FOLIO\s+FISCAL[:\s]+([A-F0-9\-\s]{20,})', full_text, re.I)
    if m:
        raw = re.sub(r'[^A-F0-9]', '', m.group(1).upper())[:32]
        if len(raw) == 32:
            data["UUID"] = f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

    # Folio de factura (no confundir con UUID)
    m = re.search(r'FACTURA\s+FOLIO:\s*(\d+)', full_text, re.I)
    if m:
        data["Folio"] = m.group(1).strip()

    # Lugar de expedición / Fechas
    m = re.search(r'LUGAR\s+DE\s+EXPEDICION:\s*(\d{5})', full_text, re.I)
    if m: data["LugarExpedicion"] = m.group(1).strip()

    m = re.search(r'FECHA:\s*(\d{4}-\d{2}-\d{2})', full_text, re.I)
    if m:
        fecha = m.group(1)
        mh = re.search(r'\b(\d{2}:\d{2}:\d{2})\b', full_text[m.end():m.end()+60])
        data["Fecha"] = parse_iso_datetime(f"{fecha}T{(mh.group(1) if mh else '00:00:00')}")

    m = re.search(r'FECHA\s+Y\s+HORA\s+DE\s+CERTIFICACION[:\s]+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        ts = m.group(1).replace(' ', 'T')
        data["FechaTimbrado"] = parse_iso_datetime(ts)

    # Forma, Método, Tipo, Moneda, Exportación
    m = re.search(r'FORMA\s+DE\s+PAGO:\s*([^\r\n]+)', full_text, re.I)
    if m: data["FormaPago"] = m.group(1).strip()
    m = re.search(r'METODO\s+DE\s+PAGO:\s*([^\r\n]+)', full_text, re.I)
    if m: data["MetodoPago"] = m.group(1).strip()
    m = re.search(r'TIPO\s+DE\s+COMPROBANTE:\s*([IEPNT])', full_text, re.I)
    if m: data["TipoDeComprobante"] = m.group(1).upper()

    # Subtotal / Total
    m = re.search(r'SUBTOTAL:\s*([0-9,]+\.\d+)', full_text, re.I)
    if m: data["SubTotal"] = dec_from_money(m.group(1))
    m = re.search(r'\bTOTAL:\s*([0-9,]+\.\d+)', full_text, re.I)
    if m: data["Total"] = dec_from_money(m.group(1))

    # ========== DESTINO para TESORO ==========
    # El destino viene en "TITULO DE COMERCIALIZACIÓN" o en ZONA
    # Ejemplo: TITULO DE COMERCIALIZACIÓN: "H/19873/COM/2017"
    m_destino = re.search(r'TITULO\s+DE\s+COMERCIALIZACION:\s*["\']?([A-Z0-9/]+)["\']?', full_text, re.I)
    if m_destino:
        data["Destino"] = m_destino.group(1).strip()
    else:
        # Alternativa: buscar en ZONA (Ej: TV17, TV18)
        m_zona = re.search(r'ZONA[:\s]+([A-Z0-9]+)', full_text, re.I)
        if m_zona:
            data["Destino"] = m_zona.group(1).strip()

    # ========== REMISIÓN para TESORO ==========
    # Buscar "COMPROBANTE CARGA: 451238312"
    m_remision = re.search(r'COMPROBANTE\s+CARGA:\s*(\d+)', full_text, re.I)
    if m_remision:
        data["Remision"] = m_remision.group(1).strip()
    else:
        # Alternativa en Addenda
        m_remision = re.search(r'Comprobante\s+de\s+Carga\s+(\d+)', full_text, re.I)
        if m_remision:
            data["Remision"] = m_remision.group(1).strip()

    # Normalizaciones
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c("MXN")
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante",""))

    return data

# extractors.py

def extract_with_profile_essafuel(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil ESSAFUEL."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",
        "Remision": ""
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === EMISOR ===
    m = re.search(r'(ESSA\s+FUEL\s+ADVISORS)', full_text, re.I)
    if m:
        data["EmisorNombre"] = m.group(1).strip()

    m = re.search(r'\b(EFA\d{6}[A-Z0-9]{3})\b', full_text)
    if m:
        data["EmisorRFC"] = m.group(1).strip()

    m = re.search(r'Regimen\s+Fiscal:\s*(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()

    # === RECEPTOR ===
    # En ESSAFUEL, después de "Receptor" aparecen DOS bloques:
    # 1. Emisor (ESSA FUEL ADVISORS con su RFC)
    # 2. Receptor real (TOTAL GAS DE... con su RFC)
    # Necesitamos el SEGUNDO bloque
    
    # Buscar todos los RFCs después de "Receptor"
    receptor_section = full_text[full_text.find("Receptor"):] if "Receptor" in full_text else full_text
    rfcs = re.findall(r'([A-Z]{3}\d{6}[A-Z0-9]{3})', receptor_section)
    
    # El primer RFC es del emisor, el segundo es del receptor
    if len(rfcs) >= 2:
        data["ReceptorRfc"] = rfcs[1]
        
        # Buscar el nombre antes del segundo RFC
        # Buscar texto entre el primer RFC y el segundo RFC
        rfc1_pos = receptor_section.find(rfcs[0])
        rfc2_pos = receptor_section.find(rfcs[1])
        if rfc1_pos >= 0 and rfc2_pos > rfc1_pos:
            between_text = receptor_section[rfc1_pos:rfc2_pos]
            # Buscar líneas con mayúsculas (nombre del receptor)
            lines_text = [l.strip() for l in between_text.split('\n') if l.strip()]
            for line_text in lines_text:
                if re.match(r'^[A-Z][A-Z\s]+$', line_text) and len(line_text) > 5:
                    data["ReceptorNombre"] = line_text
                    break
    elif len(rfcs) == 1:
        # Si solo hay un RFC, probablemente sea el receptor
        data["ReceptorRfc"] = rfcs[0]
        m = re.search(r'([A-Z][A-Z\s]+?)\s+RFC:\s*' + re.escape(rfcs[0]), receptor_section)
        if m:
            data["ReceptorNombre"] = " ".join(m.group(1).strip().split())

    # === FOLIO ===
    # Buscar "ESSA - ###", "ESSAFOC - ####" o "EFAG - ###"
    m = re.search(r'(ESSA[A-Z]*|EFAG)\s*-\s*(\d+)', full_text, re.I)
    if m:
        prefix = m.group(1).upper()  # ESSA, ESSAFOC, ESSAFFP, EFAG, etc.
        numero = m.group(2)
        data["Folio"] = f"{prefix}-{numero}"

    # === UUID ===
    m = re.search(r'([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).upper()

    # === FECHA EMISIÓN ===
    m = re.search(r'Fecha\s+Emision:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        data["Fecha"] = parse_iso_datetime(m.group(1))

    # === FECHA CERTIFICACIÓN/TIMBRADO ===
    m = re.search(r'Fecha\s+Certificacion:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        data["FechaTimbrado"] = parse_iso_datetime(m.group(1))

    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'Lugar\s+Expedicion:\s*(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()

    # === TIPO COMPROBANTE ===
    m = re.search(r'Tipo\s+Comprobante:\s*([IEPNT])\s*-\s*', full_text, re.I)
    if m:
        data["TipoDeComprobante"] = m.group(1).upper()

    # === MÉTODO DE PAGO ===
    m = re.search(r'Metodo\s+de\s+pago:\s*([A-Z]{3})', full_text, re.I)
    if m:
        data["MetodoPago"] = m.group(1).strip()

    # === FORMA DE PAGO ===
    m = re.search(r'Forma\s+(?:de\s+)?[Pp]ago:\s*(\d{2})', full_text, re.I)
    if m:
        data["FormaPago"] = m.group(1).strip()

    # === SUBTOTAL ===
    # Buscar después de la palabra "Subtotal" en la columna derecha
    m = re.search(r'Subtotal\s+\$([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))

    # === TOTAL ===
    # Para complementos de pago, buscar "Total" seguido del monto
    # Puede aparecer como "Total $625,148.93" o en línea separada
    m = re.search(r'Total\s+\$([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["Total"] = dec_from_money(m.group(1))
    else:
        # Buscar en "Importe Pagado" de documentos relacionados
        m = re.search(r'Importe\s+Pagado[^\d]*\$?([0-9,]+\.\d+)', full_text, re.I)
        if m:
            data["Total"] = dec_from_money(m.group(1))

    # === DESTINO ===
    # Buscar permiso en observaciones
    m = re.search(r'Permiso:\s*([A-Z0-9/]+)', full_text, re.I)
    if m:
        data["Destino"] = m.group(1).strip()
    else:
        # Alternativa: buscar "Entrega hecha en"
        m = re.search(r'Entrega\s+hecha\s+en\s*:\s*([^,\n]+)', full_text, re.I)
        if m:
            data["Destino"] = m.group(1).strip()

    # === REMISIÓN ===
    # Buscar BOL (Bill of Lading)
    m = re.search(r'BOL:\s*(\d+)', full_text, re.I)
    if m:
        data["Remision"] = m.group(1).strip()
    else:
        # Alternativa: DODA
        m = re.search(r'DODA:\s*(\d+)', full_text, re.I)
        if m:
            data["Remision"] = m.group(1).strip()

    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c("MXN")
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data


def extract_with_profile_premiergas(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil PREMIERGAS."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "I", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",
        "Remision": ""
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === EMISOR ===
    data["EmisorNombre"] = "PREMIERGAS SAPI DE CV"
    
    m = re.search(r'R\.F\.C\.\s+(PRE\d{9})', full_text)
    if m:
        data["EmisorRFC"] = m.group(1).strip()
    
    m = re.search(r'Reg\.\s+Fiscal:\s*(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()

    # === RECEPTOR ===
    # Buscar nombre después de "NOMBRE:"
    m = re.search(
        r'REPRESENTACION\s+IMPRESA\s+DE\s+UN\s+CFDI\.?\s*\n\s*([A-Z][A-Z\s]+)',
        full_text,
        re.I
    )
    if m:
        nombre = m.group(1).strip()
        # Limpiar: tomar solo hasta el primer salto de línea
        nombre = nombre.split('\n')[0].strip()
        # Limpiar espacios múltiples
        nombre = " ".join(nombre.split())
        data["ReceptorNombre"] = nombre
    
    # RFC del receptor - buscar el RFC que NO sea PRE190706416 (emisor)
    rfcs = re.findall(r'\b([A-Z]{3,4}\d{6}[A-Z0-9]{3})\b', full_text)
    for rfc in rfcs:
        if rfc != "PRE190706416" and not rfc.startswith("PRE"):
            data["ReceptorRfc"] = rfc
            break
    
    # RFC del receptor (buscar después de CLIENTE)
    m = re.search(r'RFC:\s+([A-Z]{3}\d{6}[A-Z0-9]{3})', full_text)
    if m:
        data["ReceptorRfc"] = m.group(1).strip()

    # === FOLIO ===
    # Formato: FE - 030621 o FE-030621
    m = re.search(r'FE\s*[-–]\s*(\d+)', full_text, re.I)
    if m:
        data["Folio"] = f"FE-{m.group(1)}"

    # === UUID ===
    m = re.search(r'Folio\s+Fiscal:\s*([A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).lower()

    # === FECHA EMISIÓN ===
    # Buscar la fecha que aparece después de "FACTURA 4.0"
    m = re.search(r'FACTURA\s+4\.0\s+FE\s*[-–]\s*\d+\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        fecha_str = m.group(1)
        # Convertir de dd/mm/yyyy hh:mm:ss a yyyy-mm-dd hh:mm:ss
        dia, mes, anio_hora = fecha_str.split('/')
        anio, hora = anio_hora.split(' ', 1)
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["Fecha"] = parse_iso_datetime(fecha_iso)

    # === FECHA TIMBRADO ===
    m = re.search(r'Fecha\s+Hora\s+de\s+Certificacion:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        fecha_str = m.group(1)
        dia, mes, anio_hora = fecha_str.split('/')
        anio, hora = anio_hora.split(' ', 1)
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)

    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'LUGAR\s+DE\s+EXPEDICION:\s*\(C\.P\.\)\s+(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()

    # === FORMA DE PAGO ===
    m = re.search(r'FORMA\s+DE\s+PAGO:\s*(\d{2})', full_text, re.I)
    if m:
        data["FormaPago"] = m.group(1).strip()

    # === MÉTODO DE PAGO ===
    m = re.search(r'METODO\s+DE\s+PAGO:\s*([A-Z]{3})', full_text, re.I)
    if m:
        data["MetodoPago"] = m.group(1).strip()

    # === SUBTOTAL ===
    m = re.search(r'Importe\s+con\s+Letras[^)]+\)\s*([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))
    else:
        # Alternativa: buscar en columna IMPORTE de la tabla de conceptos
        # Es el último valor antes de las leyendas de pago
        m = re.search(r'IMPORTE\s+([0-9,]+\.\d+)', full_text, re.I)
        if m:
            data["SubTotal"] = dec_from_money(m.group(1))

    # === TOTAL ===
    m = re.search(r'I\.V\.A\.?\s+Total:\s*([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["Total"] = dec_from_money(m.group(1))
    else:
        # Alternativa: buscar "LA CANTIDAD DE $"
        m = re.search(r'LA\s+CANTIDAD\s+DE\s+\$\s*([0-9,]+\.\d+)', full_text, re.I)
        if m:
            data["Total"] = dec_from_money(m.group(1))

    # === DESTINO ===
    # Buscar el permiso (PL/...)
    m = re.search(r'(PL/\d+/[A-Z]+(?:/[A-Z]+)?/\d{4})', full_text, re.I)
    if m:
        data["Destino"] = m.group(1).strip()

    # === REMISIÓN ===
    # Buscar el RP-#######
    m = re.search(r'(RP-\d+)', full_text, re.I)
    if m:
        data["Remision"] = m.group(1).strip()

    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c("MXN")
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = "I"  # Siempre es Ingreso

    return data




def extract_with_profile_petrotal(doc: fitz.Document) -> Dict[str, Any]:
    """Extrae campos requeridos usando el perfil PETROTAL."""
    data = {
        "Folio": "", "Fecha": None, "FormaPago": "", "MetodoPago": "",
        "Moneda": "MXN", "SubTotal": Decimal('0'), "Total": Decimal('0'),
        "Exportacion": "01", "TipoDeComprobante": "", "LugarExpedicion": "",
        "EmisorNombre": "", "EmisorRFC": "", "EmisorRegimenFiscal": "",
        "ReceptorNombre": "", "ReceptorRfc": "", "FechaTimbrado": None, "UUID": "",
        "Destino": "",
        "Remision": ""
    }

    full_text = _strip_diacritics(" ".join([p.get_text() or "" for p in doc]))

    # === EMISOR ===
    data["EmisorNombre"] = "PETROTAL"
    
    m = re.search(r'\b(PET\d{6}[A-Z0-9]{3})\b', full_text)
    if m:
        data["EmisorRFC"] = m.group(1).strip()
    
    m = re.search(r'REGIMEN:\s*(\d{3})', full_text, re.I)
    if m:
        data["EmisorRegimenFiscal"] = m.group(1).strip()

    # === RECEPTOR RFC - CORREGIDO PARA PERSONAS FÍSICAS ===
    # Patrón flexible: 3 o 4 letras + guión opcional + 6 dígitos + guión opcional + 3 caracteres
    m_rfc = re.search(r'FACTURADO\s+A\s+RFC:\s*\n\s*([A-Z\s]+?)\s*\n\s*([A-Z]{3,4}[-]?\d{6}[-]?[A-Z0-9]{3})', full_text, re.I)
    if m_rfc:
        data["ReceptorNombre"] = m_rfc.group(1).strip()
        rfc_con_guiones = m_rfc.group(2).strip()
        data["ReceptorRfc"] = rfc_con_guiones.replace('-', '')
    else:
        # Intento alternativo: buscar RFC con formato flexible
        m_rfc = re.search(r'([A-Z]{3,4}[-]?\d{6}[-]?[A-Z0-9]{3})', full_text)
        if m_rfc:
            rfc_con_guiones = m_rfc.group(1).strip()
            # Verificar que no sea el emisor
            if not rfc_con_guiones.startswith('PET'):
                data["ReceptorRfc"] = rfc_con_guiones.replace('-', '')
        
        # Buscar nombre del receptor en línea separada
        m_nombre = re.search(r'FACTURADO\s+A\s+RFC:\s*\n\s*([A-Z][A-Z\s]+?)(?=\n|$)', full_text, re.I)
        if m_nombre:
            # Limpiar el nombre (eliminar RFC si quedó pegado)
            nombre = m_nombre.group(1).strip()
            # Remover cualquier RFC que pueda estar al final
            nombre = re.sub(r'[A-Z]{3,4}[-]?\d{6}[-]?[A-Z0-9]{3}$', '', nombre).strip()
            data["ReceptorNombre"] = nombre

    # === FOLIO - CORREGIDO PARA VPET Y PET ===
    m = re.search(r'FACTURA\s+(V?PET\d+)', full_text, re.I)
    if m:
        data["Folio"] = m.group(1).strip()

    # === UUID ===
    m = re.search(r'Folio\s+Fiscal:\s*([A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12})', full_text, re.I)
    if m:
        data["UUID"] = m.group(1).upper()

    # === FECHA EMISIÓN - CORREGIDO ===
    m = re.search(r'Fecha\s+y\s+Hora\s+de\s+emision:\s*(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        dia, mes, anio = m.group(1).split('/')
        hora = m.group(2)
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["Fecha"] = parse_iso_datetime(fecha_iso)
    else:
        # Alternativa: buscar todas las fechas, la segunda es emisión
        fechas = list(re.finditer(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text))
        if len(fechas) >= 2:
            m = fechas[1]  # Segunda fecha
            dia, mes, anio = m.group(1).split('/')
            hora = m.group(2)
            fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
            data["Fecha"] = parse_iso_datetime(fecha_iso)

    # === FECHA TIMBRADO - CORREGIDO ===
    m = re.search(r'Fecha\s+y\s+Hora\s+de\s+cert\.?\s*:\s*(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text, re.I)
    if m:
        dia, mes, anio = m.group(1).split('/')
        hora = m.group(2)
        fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
        data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)
    else:
        # Alternativa: la primera fecha del documento es timbrado
        fechas = list(re.finditer(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', full_text))
        if len(fechas) >= 1:
            m = fechas[0]  # Primera fecha
            dia, mes, anio = m.group(1).split('/')
            hora = m.group(2)
            fecha_iso = f"{anio}-{mes}-{dia}T{hora}"
            data["FechaTimbrado"] = parse_iso_datetime(fecha_iso)

    # === LUGAR EXPEDICIÓN ===
    m = re.search(r'Lugar\s+de\s+Expedicion:\s*(\d{5})', full_text, re.I)
    if m:
        data["LugarExpedicion"] = m.group(1).strip()

    # === TIPO COMPROBANTE ===
    m = re.search(r'Tipo\s+Comp\.\s*:\s*([IEPNT])', full_text, re.I)
    if m:
        data["TipoDeComprobante"] = m.group(1).upper()

    # === FORMA DE PAGO ===
    m = re.search(r'FORMA\s+DE\s+PAGO:\s*(\d{2})', full_text, re.I)
    if m:
        data["FormaPago"] = m.group(1).strip()

    # === MÉTODO DE PAGO ===
    m = re.search(r'METODO\s+DE\s+PAGO:\s*([A-Z]{3})', full_text, re.I)
    if m:
        data["MetodoPago"] = m.group(1).strip()

    # === SUBTOTAL ===
    m = re.search(r'SUBTOTAL:\s*\$([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["SubTotal"] = dec_from_money(m.group(1))

    # === TOTAL ===
    m = re.search(r'TOTAL:\s*\$([0-9,]+\.\d+)', full_text, re.I)
    if m:
        data["Total"] = dec_from_money(m.group(1))

    # === DESTINO ===
    # Buscar permiso PL/
    m = re.search(r'(PL/\d+/[A-Z]+/[A-Z]+/\d{4})', full_text, re.I)
    if m:
        permiso = m.group(1).strip()
        # Buscar texto antes del permiso (puede ser estación o descripción)
        text_before = full_text[:m.start()]
        # Buscar la última línea antes del permiso que tenga mayúsculas
        lines = text_before.split('\n')
        for line in reversed(lines):
            line_clean = line.strip()
            # Buscar palabras en mayúsculas que no sean etiquetas comunes
            if re.search(r'[A-Z]{2,}', line_clean) and not any(x in line_clean.upper() for x in ['PRECIO', 'IMPORTE', 'CANT', 'CLAVE']):
                # Extraer solo el texto relevante
                m_station = re.search(r'([A-Z][A-Z\s]+?)(?=\s+\d|$)', line_clean)
                if m_station:
                    estacion = m_station.group(1).strip()
                    if len(estacion) > 3:  # Evitar capturas muy cortas
                        data["Destino"] = f"{estacion} {permiso}"
                        break
        
        if not data["Destino"]:
            data["Destino"] = permiso

    # === REMISIÓN ===
    m = re.search(r'(H/\d+/COM/\d{4})', full_text, re.I)
    if m:
        data["Remision"] = m.group(1).strip()

    # Normalizaciones finales
    data["FormaPago"] = _solo_numeros_forma_pago(data["FormaPago"])
    data["MetodoPago"] = _solo_siglas_metodo_pago(data["MetodoPago"])
    data["Moneda"] = _moneda_3c("MXN")
    data["Exportacion"] = _exportacion_code("01")
    data["TipoDeComprobante"] = _tipo_comprobante_code(data.get("TipoDeComprobante", ""))

    return data


