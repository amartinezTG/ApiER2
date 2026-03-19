# api/modelos/ImportadorFacturas.py
"""
Modelo para insertar facturas y conceptos en TG.dbo.FacturasRecibidas
"""
import pyodbc
from typing import Dict, Any, Optional, List
from decimal import Decimal
from api.db_connections import CONTROLGASTG_CONN_STR


INSERT_FACTURA = """
    INSERT INTO FacturasRecibidas (
        Folio, Fecha, FormaPago, MetodoPago, Moneda, SubTotal, Total,
        Exportacion, TipoDeComprobante, LugarExpedicion,
        EmisorNombre, EmisorRfc, EmisorRegimenFiscal,
        ReceptorNombre, ReceptorRfc,
        FechaTimbrado, UUID, Destino, Remision,
        RutaArchivo, NombreArchivo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_CONCEPTO = """
    INSERT INTO [TG].[dbo].[FacturasRecibidasConceptos]
    ([FacturaId],[Cantidad],[ClaveProdServ],[ClaveUnidad],[Descripcion],
     [ValorUnitario],[Importe],[NoIdentificacion],[ObjetoImp],[Impuesto],
     [TasaOCuota],[TipoFactor],[Base],[Unidad],[ImporteImpuesto])
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


class ImportadorFacturas:
    def __init__(self):
        self.conn_str = CONTROLGASTG_CONN_STR

    def uuid_existe(self, uuid: str) -> bool:
        """Verifica si el UUID ya existe en la base de datos."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(1) FROM FacturasRecibidas WHERE UUID = ?", (uuid,)
            )
            count = cursor.fetchone()[0]
            return count > 0

    def insertar_factura(self, d: Dict[str, Any]) -> Optional[int]:
        """Inserta cabecera de factura y retorna su ID."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(INSERT_FACTURA, (
                    d.get("Folio", ""),
                    d.get("Fecha"),
                    d.get("FormaPago", ""),
                    d.get("MetodoPago", ""),
                    d.get("Moneda", "MXN"),
                    d.get("SubTotal", Decimal("0")),
                    d.get("Total", Decimal("0")),
                    d.get("Exportacion", "01"),
                    (d.get("TipoDeComprobante") or "")[:1],
                    d.get("LugarExpedicion", ""),
                    d.get("EmisorNombre", ""),
                    d.get("EmisorRFC", ""),
                    d.get("EmisorRegimenFiscal", ""),
                    d.get("ReceptorNombre", ""),
                    d.get("ReceptorRfc", ""),
                    d.get("FechaTimbrado"),
                    d.get("UUID", ""),
                    d.get("Destino", ""),
                    d.get("Remision", ""),
                    d.get("RutaArchivo", ""),
                    d.get("NombreArchivo", ""),
                ))
                factura_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
                conn.commit()
                return int(factura_id)
            except Exception as e:
                conn.rollback()
                raise e

    def insertar_conceptos(self, factura_id: int, conceptos: List[Dict[str, Any]]) -> int:
        """Inserta los conceptos de una factura. Retorna cantidad insertada."""
        insertados = 0
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            for c in conceptos:
                try:
                    cursor.execute(INSERT_CONCEPTO, (
                        factura_id,
                        c.get("Cantidad"),
                        c.get("ClaveProdServ"),
                        c.get("ClaveUnidad"),
                        c.get("Descripcion"),
                        c.get("ValorUnitario"),
                        c.get("Importe"),
                        c.get("NoIdentificacion"),
                        c.get("ObjetoImp"),
                        c.get("Impuesto"),
                        c.get("TasaOCuota"),
                        c.get("TipoFactor"),
                        c.get("Base"),
                        c.get("Unidad"),
                        c.get("ImporteImpuesto"),
                    ))
                    conn.commit()
                    insertados += 1
                except Exception:
                    conn.rollback()
            return insertados

    def normalizar_factura(self, d: Dict[str, Any]) -> Dict[str, Any]:
        d["Folio"] = (d.get("Folio") or "")[:50]
        d["FormaPago"] = (d.get("FormaPago") or "")[:10]
        d["MetodoPago"] = (d.get("MetodoPago") or "")[:10]
        d["Moneda"] = (d.get("Moneda") or "MXN")[:3]
        d["Exportacion"] = (d.get("Exportacion") or "01")[:10]
        d["UUID"] = (d.get("UUID") or "")[:50]
        d["EmisorNombre"] = (d.get("EmisorNombre") or "")[:255]
        d["EmisorRFC"] = (d.get("EmisorRFC") or "")[:13]
        d["EmisorRegimenFiscal"] = (d.get("EmisorRegimenFiscal") or "")[:10]
        d["ReceptorNombre"] = (d.get("ReceptorNombre") or "")[:255]
        d["ReceptorRfc"] = (d.get("ReceptorRfc") or "")[:13]
        d["Destino"] = (d.get("Destino") or "")[:255]
        d["Remision"] = (d.get("Remision") or "")[:100]
        return d

    def normalizar_concepto(self, c: Dict[str, Any]) -> Dict[str, Any]:
        c["ClaveProdServ"] = (c.get("ClaveProdServ") or "")[:10]
        c["ClaveUnidad"] = (c.get("ClaveUnidad") or "")[:10]
        c["Descripcion"] = (c.get("Descripcion") or "")[:255]
        c["NoIdentificacion"] = (c.get("NoIdentificacion") or "")[:50]
        c["ObjetoImp"] = (c.get("ObjetoImp") or "")[:10]
        c["Impuesto"] = (c.get("Impuesto") or "")[:10]
        c["TipoFactor"] = (c.get("TipoFactor") or "")[:10]
        c["Unidad"] = (c.get("Unidad") or "")[:50]
        c["Cantidad"] = c.get("Cantidad") or Decimal("0")
        c["ValorUnitario"] = c.get("ValorUnitario") or Decimal("0")
        c["Importe"] = c.get("Importe") or Decimal("0")
        c["TasaOCuota"] = c.get("TasaOCuota") or Decimal("0.160000")
        c["Base"] = c.get("Base") or (c["Importe"] if isinstance(c["Importe"], Decimal) else Decimal("0"))
        c["ImporteImpuesto"] = c.get("ImporteImpuesto") or (c["Base"] * c["TasaOCuota"])
        return c
