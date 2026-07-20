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

UPDATE_FACTURA = """
    UPDATE FacturasRecibidas SET
        Folio = ?, Fecha = ?, FormaPago = ?, MetodoPago = ?, Moneda = ?,
        SubTotal = ?, Total = ?, Exportacion = ?, TipoDeComprobante = ?,
        LugarExpedicion = ?, EmisorNombre = ?, EmisorRfc = ?, EmisorRegimenFiscal = ?,
        ReceptorNombre = ?, ReceptorRfc = ?, FechaTimbrado = ?, Destino = ?, Remision = ?
    WHERE Id = ?
"""


class ImportadorFacturas:
    # Campos de cabecera que, si están vacíos en BD pero la nueva extracción
    # los trae, justifican actualizar una factura ya importada.
    CAMPOS_CLAVE = ["Folio", "Fecha", "FormaPago", "MetodoPago", "LugarExpedicion"]

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

    def obtener_factura_por_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Devuelve la cabecera resumida de la factura con ese UUID, o None si no existe."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT Id, SubTotal, Total, Folio, Fecha, FormaPago, MetodoPago, LugarExpedicion "
                "FROM FacturasRecibidas WHERE UUID = ?", (uuid,)
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "Id": row[0], "SubTotal": row[1], "Total": row[2],
                "Folio": row[3], "Fecha": row[4], "FormaPago": row[5],
                "MetodoPago": row[6], "LugarExpedicion": row[7],
            }

    def factura_incompleta(self, factura: Dict[str, Any]) -> bool:
        """Una factura se considera incompleta si su SubTotal o Total quedaron en 0."""
        subtotal = factura.get("SubTotal") or Decimal("0")
        total = factura.get("Total") or Decimal("0")
        return subtotal == 0 or total == 0

    def campos_completables(self, factura_bd: Dict[str, Any], datos: Dict[str, Any]) -> List[str]:
        """
        Campos clave vacíos en la factura de BD que la extracción nueva sí trae.
        Si la extracción tampoco los trae, no cuentan: así los proveedores cuyo
        PDF no incluye un campo siguen respondiendo 'duplicada' sin re-actualizar
        en cada corrida.
        """
        completables = []
        for campo in self.CAMPOS_CLAVE:
            vacio_en_bd = not (str(factura_bd.get(campo) or "").strip()) if campo != "Fecha" else factura_bd.get("Fecha") is None
            if vacio_en_bd and datos.get(campo):
                completables.append(campo)
        return completables

    def tiene_conceptos(self, factura_id: int) -> bool:
        """Verifica si la factura ya tiene conceptos insertados."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(1) FROM [TG].[dbo].[FacturasRecibidasConceptos] WHERE FacturaId = ?",
                (factura_id,)
            )
            return cursor.fetchone()[0] > 0

    def actualizar_factura(self, factura_id: int, d: Dict[str, Any]) -> None:
        """Actualiza la cabecera de una factura existente con datos recién extraídos."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UPDATE_FACTURA, (
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
                    d.get("Destino", ""),
                    d.get("Remision", ""),
                    factura_id,
                ))
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

    def actualizar_ruta_archivo(self, factura_id: int, ruta: str, nombre_archivo: str) -> None:
        """Actualiza RutaArchivo y NombreArchivo tras mover/renombrar el PDF en disco."""
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "UPDATE FacturasRecibidas SET RutaArchivo = ?, NombreArchivo = ? WHERE Id = ?",
                    (ruta, nombre_archivo, factura_id),
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

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
