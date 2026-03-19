# utils.py
"""
Funciones utilitarias para normalización y helpers
"""
import re
import unicodedata
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from shutil import move as _shutil_move
from typing import Any, Optional


def _strip_diacritics(s: str) -> str:
    """Elimina acentos y diacríticos del texto"""
    nkfd = unicodedata.normalize('NFKD', s or '')
    return ''.join(ch for ch in nkfd if not unicodedata.combining(ch))


def _solo_numeros_forma_pago(valor: str) -> str:
    """
    Extrae solo los dígitos de la 'Forma de Pago'.
    Ej: "99 - Por definir" -> "99"
    """
    if not valor:
        return ''
    m = re.search(r'(\d+)', valor.strip())
    return m.group(1) if m else ''


def _solo_siglas_metodo_pago(valor: str) -> str:
    """Extrae solo las siglas del método de pago (PPD, PUE, etc)"""
    if not valor:
        return ''
    m = re.search(r'([A-Z]{3})', valor.strip().upper())
    return m.group(1).lower() if m else ''


def _moneda_3c(valor: str) -> str:
    """Extrae código de moneda de 3 caracteres"""
    if not valor:
        return 'MXN'
    m = re.search(r'([A-Z]{3})', valor.strip().upper())
    return m.group(1) if m else 'MXN'


def _exportacion_code(valor: str) -> str:
    """Extrae código de exportación"""
    if not valor:
        return '01'
    v = str(valor).strip()
    m = re.search(r'(\d{2})', v)
    if m:
        return m.group(1)
    
    v_lower = v.lower()
    if 'no aplica' in v_lower:
        return '01'
    if 'definitiv' in v_lower:
        return '02'
    if 'temporal' in v_lower:
        return '03'
    return '01'


def _tipo_comprobante_code(valor: str) -> str:
    """Extrae solo la letra del tipo de comprobante"""
    if not valor:
        return ''
    m = re.search(r'([IEPNT])', valor.strip().upper())
    return m.group(1) if m else ''


def dec_from_money(s: str) -> Decimal:
    """Convierte string con formato de dinero a Decimal"""
    if not s:
        return Decimal('0')
    s = s.replace('$', '').replace(',', '').strip()
    try:
        return Decimal(s)
    except:
        return Decimal('0')


def parse_iso_datetime(s: str) -> Optional[datetime]:
    """Parse fecha ISO: 2025-09-16T22:46:09"""
    if not s:
        return None
    try:
        return datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S')
    except:
        return None


def _to_dec(s: Any, default='0', prec: int = None) -> Decimal:
    """Convierte a Decimal limpiando $ , espacios; opcionalmente redondea a 'prec' decimales."""
    if s is None:
        s = default
    s = str(s).replace('$','').replace(',','').strip()
    try:
        d = Decimal(s)
        if prec is not None:
            q = Decimal('1.' + '0'*prec)
            d = d.quantize(q)
        return d
    except:
        return Decimal(default)


def _clean(s: Any) -> str:
    """Limpia y convierte a string"""
    return (str(s or '')).strip()


def ensure_dir(path: Path) -> None:
    """Crea la carpeta si no existe (incluye padres)."""
    path.mkdir(parents=True, exist_ok=True)


def move_processed_file(src: Path, dst_dir: Path, new_name: str = None) -> Path:
    """
    Mueve el archivo src a dst_dir. 
    Si se proporciona new_name, renombra el archivo.
    Si existe un nombre duplicado, agrega sufijo con timestamp.
    
    Args:
        src: Archivo fuente
        dst_dir: Directorio destino
        new_name: Nuevo nombre para el archivo (opcional, sin extensión)
    
    Returns:
        Path del archivo movido
    """
    ensure_dir(dst_dir)
    
    # Usar nuevo nombre si se proporciona, sino mantener original
    if new_name:
        # Limpiar el UUID de caracteres no válidos para nombres de archivo
        new_name = new_name.replace("-", "_")
        # Asegurar que tenga extensión .pdf
        new_name = f"{new_name}.pdf"
        dst = dst_dir / new_name
    else:
        dst = dst_dir / src.name
    
    # Si existe, agregar timestamp
    if dst.exists():
        ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        if new_name:
            dst = dst_dir / f"{Path(new_name).stem}__{ts}.pdf"
        else:
            dst = dst_dir / f"{src.stem}__{ts}{src.suffix}"
    
    _shutil_move(str(src), str(dst))
    return dst


def provider_from_path(path_dir: Path) -> str:
    """Detecta proveedor desde el nombre de la carpeta"""
    name = path_dir.name.lower()
    if "lobo" in name:return "lobo"
    if "mcg" in name:return "mcg"
    if "tesoro" in name: return "tesoro"
    if "aemsa" in name: return "aemsa"
    if "enerey" in name: return "enerey"
    if "essafuel" in name or "essa" in name: return "essafuel"
    if "premiergas" in name or "premier" in name: return "premiergas"
    if "petrotal" in name or "pet" in name: return "petrotal"  # ← AGREGAR ESTA LÍNEA
    return None  # sin pista -> que detecte por texto


def detect_provider_profile(text_all: str, provider_hint: str = None) -> str:
    """
    Si provider_hint está presente (por carpeta), úsalo.
    Si no, intenta detectar por texto.
    """
    if provider_hint in {"lobo", "mcg", "tesoro", "aemsa", "enerey","essafuel"}:
        return provider_hint

    t = _strip_diacritics(text_all).upper()
    if "PETROLIFEROS LOBO" in t or " PLO" in t:
        return "lobo"
    if "MGC MEXICO" in t or "MME141110IJ9" in t or "MGC_CFDI" in t:
        return "mcg"
    if "TESORO MEXICO SUPPLY & MARKETING" in t or "TMS1611162N5" in t:
        return "tesoro"
    if "ALTOS ENERGETICOS MEXICANOS" in t or "AEM-160511" in t or "AEM160511" in t:
        return "aemsa"
    if "ENEREY LATINOAMERICA" in t or "SGE151215F71" in t:
        return "enerey"
    if "ESSA FUEL ADVISORS" in t or "EFA1903122IA" in t or "ESSAFOC" in t:
        return "essafuel"
    if "PREMIERGAS SAPI DE CV" in t or "PRE190706416" in t:
        return "premiergas"
    if "PETROTAL" in t or "PET180213L66" in t:  # ← AGREGAR ESTA LÍNEA
        return "petrotal"
    return "desconocido"