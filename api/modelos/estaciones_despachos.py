import pyodbc
from api.db_connections import CONTROLGASTG_CONN_STR
from datetime import datetime, timedelta


class EstacionDespachos:
    def __init__(self, conn_str: str = CONTROLGASTG_CONN_STR):
        self.conn_str = conn_str

    def estaciones(self):
        sql = """
        SELECT
        t1.Servidor,t1.BaseDatos,t1.Codigo,t1.Nombre,t2.codemp
            FROM [TG].[dbo].[Estaciones] t1
			LEFT JOIN SG12.dbo.Gasolineras t2 on t1.Codigo =t2.cod
        WHERE 
        t1.Codigo not  in (0,4,20)

        ---- activa = 1 and Codigo != 0;
        """
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []

    def comparacion_despachos(self, servidor, basedatos, codigo, from_date, until_date):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.sp_comparacion_despachos ?, ?, ?, ?, ?",(servidor, basedatos, codigo, from_date, until_date))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
            return []
    def comparacion_despachos_facturados(self, servidor, basedatos, codigo):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.ComparacionDespachosFacturados ?, ?, ?", (servidor, basedatos, codigo))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
            return []
    def comparacion_despachos_facturados_sp(self, servidor, basedatos, codigo, from_date, until_date):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.sp_comparacion_despachos_facturados ?, ?, ?, ?, ?", (servidor, basedatos, codigo, from_date, until_date))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
            return []

    def comparacion_facturas(self, servidor, basedatos, codigo):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.ComparacionFacturas ?, ?, ?", (servidor, basedatos, codigo))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
            return []

    def comparacion_series_sp(self, servidor, basedatos, codigo, from_date, until_date):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.sp_comparacion_documentosc_series ?, ?, ?, ?, ?", (servidor, basedatos, codigo, from_date, until_date))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_series_sp para {codigo}: {e}")
            return []

