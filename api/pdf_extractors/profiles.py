# profiles.py
"""
Perfiles de configuración por proveedor para extracción de datos
"""

# Perfil de proveedor LOBO basado en anclas + regex
PROFILE_LOBO = {
    "provider_key": "lobo",
    "name_contains": "PETROLIFEROS LOBO",
    "anchors": {
        "uuid": {
            "anchor_text": "FOLIO FISCAL (UUID)",
            "regex": r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}',
            "rect_offset": (0, -5, 250, 30)
        },
        "folio": {
            "anchor_text": "Factura",
            "regex": r'Factura\s+(\d+)',
            "rect_offset": (0, 0, 200, 25)
        },
        "fecha_timbrado": {
            "anchor_text": "FECHA Y HORA DE CERTIFICACIÓN",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 260, 30)
        },
        "fecha_emision": {
            "anchor_text": "FECHA Y HORA DE EMISIÓN DE CFDI",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 260, 30)
        },
        "lugar_expedicion": {
            "anchor_text": "LUGAR DE EXPEDICIÓN",
            "regex": r'\d{3,6}',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "TIPO DE COMPROBANTE",
            "regex": r'TIPO DE COMPROBANTE\s+([IEPNT])',
            "rect_offset": (0, 0, 350, 30)
        },
        "forma_pago": {
            "anchor_text": "FORMA DE PAGO",
            "regex": r'FORMA DE PAGO\s+([^\r\n]*?)(?=\s*M[EÉ]TODO DE PAGO)',
            "rect_offset": (0, 0, 350, 30)
        },
        "metodo_pago": {
            "anchor_text": "MÉTODO DE PAGO",
            "regex": r'M[EÉ]TODO DE PAGO\s+([^\r\n]*?)(?=\s*CONDICIONES DE PAGO)',
            "rect_offset": (0, 0, 420, 30)
        },
        "moneda": {
            "anchor_text": "MONEDA",
            "regex": r'MONEDA\s+([^\r\n]*?)(?=\s*VERSION)',
            "rect_offset": (0, 0, 300, 30)
        },
        "exportacion": {
            "anchor_text": "EXPORTACION",
            "regex": r'EXPORTACION\s+([^\r\n]*?)(?=\s*SUBTOTAL)',
            "rect_offset": (0, 0, 300, 30)
        },
        "subtotal": {
            "anchor_text": "SUBTOTAL",
            "regex": r'SUBTOTAL\s+\$\s*([0-9][\d,]*\.\d+)',
            "rect_offset": (0, -5, 280, 35)
        },
        "total": {
            "anchor_text": "TOTAL",
            "regex": r'(?<!SUB)\bTOTAL\b\s+\$\s*([0-9][\d,]*\.\d+)',
            "rect_offset": (0, -5, 280, 35)
        },
    },
    "regex": {
        "emisor_nombre": r'(PETROLIFEROS LOBO)',
        "emisor_rfc": r'(PLO\d{6}[A-Z0-9]{3})',
        "emisor_regimen": r'R[EÉ]GIMEN FISCAL:\s*(\d{2,3})\b',
        "receptor_nombre": r'CLIENTE\s+([A-Z\s]+)\s+[A-Z]{3}\d{6}[A-Z0-9]{3}',
        "receptor_rfc": r'CLIENTE\s+[A-Z\s]+\s+([A-Z]{3}\d{6}[A-Z0-9]{3})',
    }
}

# Perfil de proveedor MCG
PROFILE_MCG = {
    "provider_key": "mcg",
    "name_contains": "MGC MEXICO",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio Fiscal",
            "regex": r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}',
            "rect_offset": (0, -5, 280, 40)
        },
        "fecha_emision": {
            "anchor_text": "Fecha Factura:",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 260, 35)
        },
        "fecha_timbrado": {
            "anchor_text": "Fecha y hora de Certificación:",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 260, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "Lugar de expedición",
            "regex": r'\d{3,6}',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "Tipo de Comprobante",
            "regex": r'Tipo de Comprobante[:\s]*([IEPNT])',
            "rect_offset": (0, 0, 200, 30)
        },
        "forma_pago": {
            "anchor_text": "Forma de pago",
            "regex": r'Forma de pago\s+([^\r\n]+?)\s+(?:Lugar|R[ée]gimen|M[ée]todo)',
            "rect_offset": (0, 0, 220, 30)
        },
        "metodo_pago": {
            "anchor_text": "Método de pago",
            "regex": r'M[ée]todo de pago\s+([^\r\n]+?)\s+(?:Forma|Lugar|R[ée]gimen)',
            "rect_offset": (0, 0, 220, 30)
        },
        "moneda": {
            "anchor_text": "Total",
            "regex": r'\b(MXN|USD|EUR)\b',
            "rect_offset": (0, -5, 160, 30)
        },
        "subtotal": {
            "anchor_text": "Subtotal",
            "regex": r'Subtotal\s+([0-9][\d,]*\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
        "total": {
            "anchor_text": "Total",
            "regex": r'Total[^\d]*([0-9][\d,]*\.\d+)',
            "rect_offset": (0, -5, 240, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(MGC\s*MEXICO)',
        "emisor_rfc": r'\b(MME141110IJ9)\b',
        "emisor_regimen": r'R[ée]gimen\s*Fiscal\s+(\d{3})',
        "receptor_nombre": r'FACTURADO A\s+([A-Z0-9\s]+?)\s+[A-Z]{3}\d{6}[A-Z0-9]{3}',
        "receptor_rfc": r'\b([A-Z]{3}\d{6}[A-Z0-9]{3})\b',
    }
}
# profiles.py
PROFILE_TESORO = {
    "provider_key": "tesoro",
    "name_contains": "TESORO MEXICO SUPPLY & MARKETING",
    "anchors": {
        "uuid": {
            "anchor_text": "FOLIO FISCAL",
            "regex": r'[A-F0-9]{8}[-\s]?[A-F0-9]{4}[-\s]?[A-F0-9]{4}[-\s]?[A-F0-9]{4}[-\s]?[A-F0-9]{12}',
            "rect_offset": (0, -5, 320, 45)
        },
        "fecha_emision": {
            "anchor_text": "FECHA:",
            "regex": r'\d{4}-\d{2}-\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "fecha_timbrado": {
            "anchor_text": "FECHA Y HORA DE CERTIFICACIÓN",
            "regex": r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 300, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "LUGAR DE EXPEDICION",
            "regex": r'\b\d{5}\b',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "TIPO DE COMPROBANTE",
            "regex": r'TIPO DE COMPROBANTE:\s*([IEPNT])',
            "rect_offset": (0, 0, 260, 30)
        },
        "forma_pago": {
            "anchor_text": "FORMA DE PAGO",
            "regex": r'FORMA DE PAGO:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 260, 30)
        },
        "metodo_pago": {
            "anchor_text": "METODO DE PAGO",
            "regex": r'METODO DE PAGO:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 260, 30)
        },
        "moneda": {
            "anchor_text": "MONEDA",
            "regex": r'\b(MXN|USD|EUR)\b',
            "rect_offset": (0, -5, 160, 30)
        },
        "subtotal": {
            "anchor_text": "SUBTOTAL:",
            "regex": r'SUBTOTAL:\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
        "total": {
            "anchor_text": "TOTAL:",
            "regex": r'TOTAL:\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(TESORO\s+MEXICO\s+SUPPLY\s*&\s*MARKETING)',
        "emisor_rfc": r'\b(TMS1611162N5)\b',
        "emisor_regimen": r'REGIMEN\s+FISCAL:\s*(\d{3})',
        "receptor_nombre": r'PARA:\s*([\w\s&.]+)\s+[A-Z]{3}\d{6}[A-Z0-9]{3}',
        "receptor_rfc": r'PARA:\s*[\w\s&.]+\s+([A-Z]{3}\d{6}[A-Z0-9]{3})',
        # Folio de factura (no es el UUID)
        "folio_factura": r'FACTURA\s+FOLIO:\s*(\d+)',
    }
}

# profiles.py
PROFILE_AEMSA = {
    "provider_key": "aemsa",
    "name_contains": "ALTOS ENERGETICOS MEXICANOS",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio Fiscal:",
            "regex": r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}',
            "rect_offset": (0, -5, 320, 40)
        },
        "folio": {
            "anchor_text": "FACTURA",
            "regex": r'FACTURA\s+(F\d+)',
            "rect_offset": (0, 0, 200, 30)
        },
        "fecha_emision": {
            "anchor_text": "Fecha del Documento",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "fecha_timbrado": {
            "anchor_text": "Fecha y Hora de cert.:",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "Lugar de Expedición",
            "regex": r'\b\d{5}\b',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "Tipo Comp.:",
            "regex": r'Tipo\s+Comp\.\s*:\s*([IEPNT])',
            "rect_offset": (0, 0, 200, 30)
        },
        "forma_pago": {
            "anchor_text": "FORMA DE PAGO:",
            "regex": r'FORMA\s+DE\s+PAGO:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 300, 30)
        },
        "metodo_pago": {
            "anchor_text": "MÉTODO DE PAGO:",
            "regex": r'M[ÉE]TODO\s+DE\s+PAGO:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 300, 30)
        },
        "moneda": {
            "anchor_text": "MONEDA:",
            "regex": r'MONEDA:\s*([A-Z]{3})',
            "rect_offset": (0, 0, 160, 30)
        },
        "subtotal": {
            "anchor_text": "SUBTOTAL:",
            "regex": r'SUBTOTAL:\s*\$?\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
        "total": {
            "anchor_text": "TOTAL:",
            "regex": r'TOTAL:\s*\$?\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(ALTOS\s+ENERGETICOS\s+MEXICANOS)',
        "emisor_rfc": r'\b(AEM[0-9]{6}[A-Z0-9]{3})\b',
        "emisor_regimen": r'R[ÉE]GIMEN:\s*(\d{3})',
        "receptor_nombre": r'FACTURADO\s+A\s+RFC:\s*([^\n]+?)\s+[A-Z]{3}[0-9-]{9,}',
        "receptor_rfc": r'FACTURADO\s+A\s+RFC:\s*[^\n]+?\s+([A-Z]{3}[0-9-]{9,})',
    }
}

PROFILE_ENEREY = {
    "provider_key": "enerey",
    "name_contains": "ENEREY LATINOAMERICA",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio SAT:",
            "regex": r'[A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12}',
            "rect_offset": (0, -5, 320, 40)
        },
        "folio": {
            "anchor_text": "Factura",
            "regex": r'Factura\s+(E\s*\d+)',
            "rect_offset": (0, 0, 200, 30)
        },
        "fecha_emision": {
            "anchor_text": "Fecha de emisión:",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "fecha_timbrado": {
            "anchor_text": "Fecha de certificación:",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "Lugar de expedición:",
            "regex": r'\b\d{5}\b',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "Tipo de Comprobante:",
            "regex": r'([IEPNT])\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "forma_pago": {
            "anchor_text": "Forma de pago:",
            "regex": r'Forma\s+de\s+pago:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 300, 30)
        },
        "metodo_pago": {
            "anchor_text": "Método de pago:",
            "regex": r'M[ée]todo\s+de\s+pago:\s*([^\r\n]+)',
            "rect_offset": (0, 0, 300, 30)
        },
        "subtotal": {
            "anchor_text": "Subtotal:",
            "regex": r'Subtotal:\s*\$?\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
        "total": {
            "anchor_text": "TOTAL:",
            "regex": r'TOTAL:\s*\$?\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(ENEREY\s+LATINOAMERICA)',
        "emisor_rfc": r'\b(SGE\d{6}[A-Z0-9]{3})\b',
        "emisor_regimen": r'R[ée]gimen\s+fiscal\s+emisor:\s*(\d{3})',
        "receptor_nombre": r'Facturado\s+a:\s*([^\n]+)',
        "receptor_rfc": r'([A-Z]{3}\d{6}[A-Z0-9]{3})',
    }
}

# profiles.py

PROFILE_ESSAFUEL = {
    "provider_key": "essafuel",
    "name_contains": "ESSA FUEL ADVISORS",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio Fiscal:",
            "regex": r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}',
            "rect_offset": (0, -5, 320, 40)
        },
        "folio": {
            "anchor_text": "ESSA",  # Captura tanto "ESSA - ###" como "ESSAFOC - ####"
            "regex": r'ESSA(?:FOC)?\s*-\s*(\d+)',
            "rect_offset": (0, 0, 200, 30)
        },
        "fecha_emision": {
            "anchor_text": "Fecha Emisión:",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 30)
        },
        "fecha_certificacion": {
            "anchor_text": "Fecha Certificación:",
            "regex": r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 30)
        },
        "lugar_expedicion": {
            "anchor_text": "Lugar Expedición:",
            "regex": r'\d{5}',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "Tipo Comprobante:",
            "regex": r'([IEPNT])\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "metodo_pago": {
            "anchor_text": "Método de pago:",
            "regex": r'([A-Z]{3})\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "forma_pago": {
            "anchor_text": "Forma de pago:",
            "regex": r'(\d{2})\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "subtotal": {
            "anchor_text": "Subtotal",
            "regex": r'\$([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 200, 30)
        },
        "total": {
            "anchor_text": "Total",
            "regex": r'\$([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 200, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(ESSA\s+FUEL\s+ADVISORS)',
        "emisor_rfc": r'\b(EFA\d{6}[A-Z0-9]{3})\b',
        "emisor_regimen": r'Régimen\s+Fiscal:\s*(\d{3})',
        "receptor_nombre": r'Receptor\s+([A-Z\s]+?)\s+RFC:',
        "receptor_rfc": r'Receptor[^\n]+RFC:\s*([A-Z]{3}\d{6}[A-Z0-9]{3})',
    }
}

# profiles.py - PREMIERGAS
PROFILE_PREMIERGAS = {
    "provider_key": "premiergas",
    "name_contains": "PREMIERGAS SAPI DE CV",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio Fiscal:",
            "regex": r'[A-F0-9a-f]{8}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{4}-[A-F0-9a-f]{12}',
            "rect_offset": (0, -5, 320, 40)
        },
        "folio": {
            "anchor_text": "FACTURA 4.0",
            "regex": r'FE\s*[-–]\s*(\d+)',
            "rect_offset": (0, 0, 200, 30)
        },
        "fecha_emision": {
            "anchor_text": "FACTURA 4.0",
            "regex": r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})',
            "rect_offset": (0, 20, 200, 50)
        },
        "fecha_timbrado": {
            "anchor_text": "Fecha Hora de Certificación:",
            "regex": r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})',
            "rect_offset": (0, -5, 200, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "LUGAR DE EXPEDICION:",
            "regex": r'(\d{5})',
            "rect_offset": (0, 10, 120, 30)
        },
        "subtotal": {
            "anchor_text": "Subtotal:",
            "regex": r'Subtotal:\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
        "total": {
            "anchor_text": "Total:",
            "regex": r'Total:\s*([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 220, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(PREMIERGAS\s+SAPI\s+DE\s+CV)',
        "emisor_rfc": r'R\.F\.C\.\s+(PRE\d{9})',
        "emisor_regimen": r'Reg\.\s+Fiscal:\s*(\d{3})',
        "receptor_nombre": r'NOMBRE:\s+([A-Z\s]+)',
        "receptor_rfc": r'RFC:\s+([A-Z]{3}\d{6}[A-Z0-9]{3})',
        "forma_pago": r'FORMA\s+DE\s+PAGO:\s*(\d{2})',
        "metodo_pago": r'METODO\s+DE\s+PAGO:\s*([A-Z]{3})',
    }
}

PROFILE_PETROTAL = {
    "provider_key": "petrotal",
    "name_contains": "PETROTAL",
    "anchors": {
        "uuid": {
            "anchor_text": "Folio Fiscal:",
            "regex": r'[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}',
            "rect_offset": (0, -5, 320, 40)
        },
        "folio": {
            "anchor_text": "FACTURA",
            "regex": r'PET\d+',
            "rect_offset": (0, 0, 200, 30)
        },
        "fecha_emision": {
            "anchor_text": "Fecha y Hora de emisión:",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "fecha_timbrado": {
            "anchor_text": "Fecha y Hora de cert.:",
            "regex": r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
            "rect_offset": (0, -5, 200, 35)
        },
        "lugar_expedicion": {
            "anchor_text": "Lugar de Expedición:",
            "regex": r'\d{5}',
            "rect_offset": (0, -5, 120, 30)
        },
        "tipo_comprobante": {
            "anchor_text": "Tipo Comp.:",
            "regex": r'([IEPNT])',
            "rect_offset": (0, 0, 200, 30)
        },
        "forma_pago": {
            "anchor_text": "FORMA DE PAGO:",
            "regex": r'(\d{2})\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "metodo_pago": {
            "anchor_text": "MÉTODO DE PAGO:",
            "regex": r'([A-Z]{3})\s*-\s*',
            "rect_offset": (0, 0, 200, 30)
        },
        "subtotal": {
            "anchor_text": "SUBTOTAL:",
            "regex": r'\$([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 200, 30)
        },
        "total": {
            "anchor_text": "TOTAL:",
            "regex": r'\$([0-9,]+\.\d+)',
            "rect_offset": (0, -5, 200, 30)
        },
    },
    "regex": {
        "emisor_nombre": r'(PETROTAL)',
        "emisor_rfc": r'\b(PET\d{6}[A-Z0-9]{3})\b',
        "emisor_regimen": r'RÉGIMEN:\s*(\d{3})',
        "receptor_nombre": r'FACTURADO\s+A\s+RFC:\s*\n\s*([A-Z\s]+?)\s+[A-Z]{3}',
        "receptor_rfc": r'FACTURADO\s+A\s+RFC:\s*\n\s*[A-Z\s]+?\s+([A-Z]{3}\d{6}[A-Z0-9]{3})',
    }
}

# Actualizar la lista al final:
PROFILES = [PROFILE_LOBO, PROFILE_MCG, PROFILE_TESORO, PROFILE_AEMSA, PROFILE_ENEREY, PROFILE_ESSAFUEL, PROFILE_PREMIERGAS, PROFILE_PETROTAL]



