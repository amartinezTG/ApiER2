from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework import status
from concurrent.futures import ThreadPoolExecutor , as_completed
from django.http import HttpResponse
from api.modelos.estaciones_despachos import EstacionDespachos
from api.modelos.Documentos_estaciones import DocumentosEstaciones
from api.modelos.inventarios_estaciones import InventariosEstaciones
from api.modelos.Facturas_Recibidas import FacturasRecibidas
from api.modelos.ImportadorFacturas import ImportadorFacturas
import logging
from collections import defaultdict
import json
import tempfile
import os

logger = logging.getLogger(__name__)


@api_view(['GET', 'POST'])
def estacion_porcentaje(request):
    print("INICIANDO estacion_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)

@api_view(['GET', 'POST'])
def porcent_estacion_facturados_info(request):
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    # MAX_THREADS = min(20, len(estaciones))            # <= 10 hilos
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos_facturados, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def porcent_facturas_info(request):
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    # MAX_THREADS = min(20, len(estaciones))            # <= 10 hilos
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_facturas, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def estacion_despachos_porcentaje(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_despachos_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def estacion_despachos_facturados_porcentaje(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_despachos_facturados_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos_facturados_sp, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)

@api_view(['GET', 'POST'])
def estacion_comparacion_series(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_comparacion_series")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_series_sp, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)
 
@api_view(['GET', 'POST'])
def estacion_documentos_compra(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    codgas = request.data.get('codgas')
    proveedor = request.data.get('proveedor')
    company = request.data.get('company')
    if not all([from_date, until_date, codgas, proveedor, company]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)

    # debug = [from_date, until_date, codgas, proveedor, company]
    # return Response(debug , status=status.HTTP_200_OK)

    documentos_estaciones = DocumentosEstaciones()
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = []

    # Convertir a enteros para comparaciones
    company_int = int(company)
    codgas_int = int(codgas)
    if company_int == 0 and codgas_int == 0:
        estaciones_filtradas = estaciones
    elif  company_int == 0 and codgas_int != 0:
        # Si company=0 pero codgas específico, filtrar solo por codgas
        estaciones_filtradas = [e for e in estaciones if e["Codigo"] == codgas_int]

    elif company_int != 0 and codgas_int == 0:
        # Si company específico pero codgas=0, filtrar solo por company
        estaciones_filtradas = [e for e in estaciones if e.get("codemp") == company_int]

    else:
        # Si ambos son específicos, filtrar por company primero y luego por codgas
        estaciones_por_empresa = [e for e in estaciones if e.get("codemp") == company_int]
        estaciones_filtradas = [e for e in estaciones_por_empresa if e["Codigo"] == codgas_int]
    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                documentos_estaciones.get_purchase_from_station, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date, proveedor
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.extend(
                     res
                )

    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def facturas_vencen_hoy(request):
    """
    Retorna facturas de compra vencidas (sin orden de pago) en un rango de fechas de vencimiento.
    Parámetros opcionales:
      - mode: 'hoy' (solo hoy, default) | 'mes' (mes en curso desde día 1 hasta hoy)
      - from_due / until_due: rango explícito YYYY-MM-DD (sobreescribe mode)
      - codgas, proveedor, company: filtros de estación/proveedor/empresa (0 = todos)
    Solo retorna facturas que NO están asignadas a una orden de pago.
    """
    from datetime import date
    today = date.today()

    mode      = request.data.get('mode', 'hoy')
    from_due  = request.data.get('from_due')
    until_due = request.data.get('until_due')
    codgas    = request.data.get('codgas',    '0')
    proveedor = request.data.get('proveedor', '0')
    company   = request.data.get('company',   '0')

    # Resolver rango según mode si no se pasaron fechas explícitas
    if not from_due or not until_due:
        if mode == 'mes':
            from_due  = today.replace(day=1).isoformat()
            until_due = today.isoformat()
        else:  # 'hoy'
            from_due  = today.isoformat()
            until_due = today.isoformat()

    documentos_estaciones = DocumentosEstaciones()
    estacion_despachos    = EstacionDespachos()
    estaciones            = estacion_despachos.estaciones()

    company_int = int(company)
    codgas_int  = int(codgas)
    prov_int    = int(proveedor)

    if company_int == 0 and codgas_int == 0:
        estaciones_filtradas = estaciones
    elif company_int == 0 and codgas_int != 0:
        estaciones_filtradas = [e for e in estaciones if e["Codigo"] == codgas_int]
    elif company_int != 0 and codgas_int == 0:
        estaciones_filtradas = [e for e in estaciones if e.get("codemp") == company_int]
    else:
        estaciones_por_empresa = [e for e in estaciones if e.get("codemp") == company_int]
        estaciones_filtradas   = [e for e in estaciones_por_empresa if e["Codigo"] == codgas_int]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                documentos_estaciones.get_overdue_invoices,
                est["Servidor"],
                est["BaseDatos"],
                est["Codigo"],
                from_due,
                until_due,
                prov_int
            ): est
            for est in estaciones_filtradas
        }
        for future in as_completed(future_to_est):
            res = future.result()
            if res:
                resultados.extend(res)

    if not resultados:
        return Response({"detail": "No hay facturas vencidas en el rango indicado"}, status=status.HTTP_404_NOT_FOUND)

    resultados.sort(key=lambda x: (x.get('proveedor') or '', x.get('gasolinera') or ''))
    return Response(resultados, status=status.HTTP_200_OK)
 

@api_view(['GET', 'POST'])
def analisis_de_compras(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    codgas = request.data.get('codgas')
    proveedor = request.data.get('proveedor')
    company = request.data.get('company')
    if not all([from_date, until_date, codgas, proveedor, company]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)

    # debug = [from_date, until_date, codgas, proveedor, company]
    # return Response(debug , status=status.HTTP_200_OK)

    documentos_estaciones = DocumentosEstaciones()
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = []

    # Convertir a enteros para comparaciones
    company_int = int(company)
    codgas_int = int(codgas)
    if company_int == 0 and codgas_int == 0:
        estaciones_filtradas = estaciones
    elif  company_int == 0 and codgas_int != 0:
        # Si company=0 pero codgas específico, filtrar solo por codgas
        estaciones_filtradas = [e for e in estaciones if e["Codigo"] == codgas_int]

    elif company_int != 0 and codgas_int == 0:
        # Si company específico pero codgas=0, filtrar solo por company
        estaciones_filtradas = [e for e in estaciones if e.get("codemp") == company_int]

    else:
        # Si ambos son específicos, filtrar por company primero y luego por codgas
        estaciones_por_empresa = [e for e in estaciones if e.get("codemp") == company_int]
        estaciones_filtradas = [e for e in estaciones_por_empresa if e["Codigo"] == codgas_int]
    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                documentos_estaciones.analisis_de_compras, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date, proveedor
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.extend(
                     res
                )

    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)






@api_view(['POST'])
def inventarios_distribuido(request):
    print("INICIANDO inventarios_distribuido")

    try:
        # Obtener parámetros
        from_date = request.data.get('from')
        until_date = request.data.get('until')
        if not from_date or not until_date:
            return Response(
                {"detail": "Los parámetros 'from' y 'until' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        # Convertir fechas a formato int si vienen como string YYYY-MM-DD
        if isinstance(from_date, str) and '-' in from_date:
            from_date = from_date.replace('-', '')
        if isinstance(until_date, str) and '-' in until_date:
            until_date = until_date.replace('-', '')


        # Obtener lista de estaciones
        estacion_despachos = EstacionDespachos()
        inventarios_model = InventariosEstaciones()
        estaciones = estacion_despachos.estaciones()
        
        if not estaciones:
            return Response(
                {"detail": "No se encontraron estaciones configuradas"},
                status=status.HTTP_404_NOT_FOUND
            )

        print(f"Total estaciones a consultar: {len(estaciones)}")

        resultados = []
        errores = 0
        
        # Consultar cada estación en paralelo
        with ThreadPoolExecutor(max_workers=40) as executor:
            future_to_est = {
                executor.submit(
                    inventarios_model.get_inventarios_estacion,
                    est["Servidor"],
                    est["BaseDatos"],
                    est["Codigo"],
                    from_date,
                    until_date
                ): est
                for est in estaciones
            }
            
            for future in as_completed(future_to_est):
                est = future_to_est[future]
                try:
                    res = future.result()
                    if res and len(res) > 0:
                        resultados.append({
                            "Estacion": est["Codigo"],
                            "Servidor": est["Servidor"],
                            "BaseDatos": est["BaseDatos"],
                            "Nombre": est["Nombre"],
                            "Resultados": res
                        })
                        print(f"✓ Estación {est['Nombre']}: {len(res)} registros")
                    else:
                        print(f"○ Estación {est['Nombre']}: Sin datos")
                except Exception as e:
                    errores += 1
                    print(f"✗ Error en estación {est['Nombre']}: {str(e)}")
                    # Continuar con las demás estaciones
                    continue

        print(f"Consulta completada: {len(resultados)} estaciones con datos, {errores} errores")

        if not resultados:
            return Response(
                {"detail": "No se encontraron resultados para el rango de fechas especificado"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(resultados, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error general en inventarios_distribuido: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    
@api_view(['POST'])
def inventarios_detalles_distribuido(request):
    """
    Consulta detalles diarios de inventarios de UNA estación específica
    """
    print("INICIANDO inventarios_detalles_distribuido")
    
    try:
        # Obtener parámetros
        from_date = request.data.get('from')
        until_date = request.data.get('until')
        codgas = request.data.get('codgas')
        codprd = request.data.get('codprd')
        
        if not all([from_date, until_date, codgas, codprd]):
            return Response(
                {"detail": "Los parámetros 'from', 'until', 'codgas' y 'codprd' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        print(f"Consultando detalles - Estación: {codgas}, Producto: {codprd}, Fechas: {from_date} - {until_date}")
        
        # Obtener información de la estación específica
        estacion_despachos = EstacionDespachos()
        inventarios_model = InventariosEstaciones()
        estaciones = estacion_despachos.estaciones()
        
        # Buscar la estación específica
        estacion = next((est for est in estaciones if est["Codigo"] == int(codgas)), None)
        
        if not estacion:
            return Response(
                {"detail": f"No se encontró la estación con código {codgas}"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        print(f"Consultando estación: {estacion['Nombre']}")
        
        # Consultar detalles
        resultados = inventarios_model.get_detalles_estacion(
            estacion["Servidor"],
            estacion["BaseDatos"],
            int(codgas),
            int(codprd),
            from_date,
            until_date
        )
        
        if not resultados or len(resultados) == 0:
            return Response(
                {"detail": "No se encontraron resultados para los parámetros especificados"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        print(f"✓ Consulta completada: {len(resultados)} registros")
        
        return Response({
            "Estacion": estacion["Codigo"],
            "Nombre": estacion["Nombre"],
            "Resultados": resultados
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error general en inventarios_detalles_distribuido: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
@api_view(['POST'])
def tanques_estacion(request):
    """
    Obtiene la lista de tanques de una estación
    """
    print("INICIANDO tanques_estacion")
    
    try:
        codgas = request.data.get('codgas')
        servidor = request.data.get('servidor')
        base = request.data.get('base')
        
        if not all([codgas, servidor, base]):
            return Response(
                {"detail": "Los parámetros 'codgas', 'servidor' y 'base' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        inventarios_model = InventariosEstaciones()
        tanques = inventarios_model.get_tanques_estacion(servidor, base, int(codgas))
        if not tanques:
            return Response(
                {"tanques": []},
                status=status.HTTP_200_OK
            )
        
        print(f"✓ Tanques encontrados: {len(tanques)}")
        
        return Response({"tanques": tanques}, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error en tanques_estacion: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@api_view(['POST'])
def volumen_tanque(request):
    """
    Obtiene el historial de volumen de un tanque
    """
    print("INICIANDO volumen_tanque")
    
    try:
        codgas = request.data.get('codgas')
        codtan = request.data.get('codtan')
        limit = request.data.get('limit', 100)
        
        if not all([codgas, codtan]):
            return Response(
                {"detail": "Los parámetros 'codgas' y 'codtan' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        print(f"Consultando volumen - Estación: {codgas}, Tanque: {codtan}, Límite: {limit}")
        
        # Obtener información de la estación
        estacion_despachos = EstacionDespachos()
        estaciones = estacion_despachos.estaciones()
        estacion = next((est for est in estaciones if est["Codigo"] == int(codgas)), None)
        
        if not estacion:
            return Response(
                {"detail": f"No se encontró la estación con código {codgas}"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        inventarios_model = InventariosEstaciones()
        resultados = inventarios_model.get_volumen_tanque(
            estacion["Servidor"],
            estacion["BaseDatos"],
            int(codgas),
            int(codtan),
            int(limit)
        )
        
        if not resultados:
            return Response(
                {"detail": "No se encontraron resultados"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        print(f"✓ Registros encontrados: {len(resultados)}")
        
        return Response({
            "Estacion": estacion["Codigo"],
            "Nombre": estacion["Nombre"],
            "Resultados": resultados
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error en volumen_tanque: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    

@api_view(['POST'])
def volumen_date(request):
    """
    Obtiene el historial de volumen de un tanque
    """
    print("INICIANDO volumen_tanque")
    
    try:
        codgas = request.data.get('codgas')
        codtan = request.data.get('codtan')
        from_date = request.data.get('from_date')
        until_date = request.data.get('until_date')
        
        if not all([codgas, codtan]):
            return Response(
                {"detail": "Los parámetros 'codgas' y 'codtan' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        print(f"Consultando volumen - Estación: {codgas}, Tanque: {codtan}")
        
        # Obtener información de la estación
        estacion_despachos = EstacionDespachos()
        estaciones = estacion_despachos.estaciones()
        estacion = next((est for est in estaciones if est["Codigo"] == int(codgas)), None)
        
        if not estacion:
            return Response(
                {"detail": f"No se encontró la estación con código {codgas}"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        inventarios_model = InventariosEstaciones()
        resultados = inventarios_model.get_volumen_date_tanque(
            estacion["Servidor"],
            estacion["BaseDatos"],
            int(codgas),
            int(codtan),
            int(from_date),
            int(until_date)
        )
        
        if not resultados:
            return Response(
                {"detail": "No se encontraron resultados"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        print(f"✓ Registros encontrados: {len(resultados)}")
        
        return Response({
            "Estacion": estacion["Codigo"],
            "Nombre": estacion["Nombre"],
            "Resultados": resultados
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error en volumen_tanque: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@api_view(['POST'])
def tanques_consolidado(request):
    """
    Consulta consolidada de todas las estaciones en paralelo
    """
    print("INICIANDO tanques_consolidado")
    
    try:
        from_date = request.data.get('from')
        until_date = request.data.get('until')
        
        if not all([from_date, until_date]):
            return Response(
                {"detail": "Los parámetros 'from' y 'until' son requeridos"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        print(f"Consultando todas las estaciones - Fechas: {from_date} a {until_date}")
        
        # Obtener todas las estaciones
        estacion_despachos = EstacionDespachos()
        inventarios_model = InventariosEstaciones()
        estaciones = estacion_despachos.estaciones()
        
        # Filtrar estaciones excluidas
        estaciones = [est for est in estaciones if est["Codigo"] not in [0, 4, 20]]
        
        if not estaciones:
            return Response(
                {"detail": "No se encontraron estaciones configuradas"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        print(f"Total estaciones a consultar: {len(estaciones)}")
        
        resultados = []
        errores = 0
        
        # Consultar cada estación en paralelo
        with ThreadPoolExecutor(max_workers=40) as executor:
            future_to_est = {
                executor.submit(
                    inventarios_model.get_consolidado_tanques,
                    est["Servidor"],
                    est["BaseDatos"],
                    est["Codigo"],
                    from_date,
                    until_date
                ): est
                for est in estaciones
            }
            
            for future in as_completed(future_to_est):
                est = future_to_est[future]
                try:
                    res = future.result()
                    if res and len(res) > 0:
                        for tanque in res:
                            resultados.append({
                                "Estacion": est["Nombre"],
                                "CodEstacion": est["Codigo"],
                                "Servidor": est["Servidor"],
                                "BaseDatos": est["BaseDatos"],
                                **tanque
                            })
                        print(f"✓ Estación {est['Nombre']}: {len(res)} tanques")
                    else:
                        print(f"○ Estación {est['Nombre']}: Sin datos")
                except Exception as e:
                    errores += 1
                    print(f"✗ Error en estación {est['Nombre']}: {str(e)}")
                    continue
        
        print(f"Consulta completada: {len(resultados)} registros de tanques, {errores} errores")
        
        if not resultados:
            return Response(
                {"detail": "No se encontraron resultados para el rango de fechas especificado"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(resultados, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error general en tanques_consolidado: {str(e)}")
        return Response(
            {"detail": f"Error interno: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    


@api_view(['GET', 'POST'])
def resumen_movimientos_tanques(request):
    """
    Endpoint para obtener resumen de movimientos de tanques con información de facturas
    """
    # Obtener parámetros
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    codgas = request.data.get('codgas')
    proveedor = request.data.get('proveedor')
    company = request.data.get('company')

    # Validación de parámetros requeridos
    if not all([from_date, until_date]):
        return Response(
            {"detail": "Faltan parámetros requeridos: from y until son obligatorios"}, 
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        # Instanciar clases
        documentos_estaciones = DocumentosEstaciones()
        estacion_despachos = EstacionDespachos()
        
        # Obtener lista de estaciones
        estaciones = estacion_despachos.estaciones()
        
        # Convertir a enteros para comparaciones
        company_int = int(company or 0)
        codgas_int = int(codgas or 0)
        
        # Filtrar estaciones según criterios
        estaciones_filtradas = []
        
        if company_int == 0 and codgas_int == 0:
            # Todas las estaciones
            estaciones_filtradas = estaciones
        elif company_int == 0 and codgas_int != 0:
            # Solo la estación específica
            estaciones_filtradas = [e for e in estaciones if e["Codigo"] == codgas_int]
        elif company_int != 0 and codgas_int == 0:
            # Todas las estaciones de la empresa
            estaciones_filtradas = [e for e in estaciones if e.get("codemp") == company_int]
        else:
            # Empresa y estación específicas
            estaciones_por_empresa = [e for e in estaciones if e.get("codemp") == company_int]
            estaciones_filtradas = [e for e in estaciones_por_empresa if e["Codigo"] == codgas_int]

        if not estaciones_filtradas:
            return Response(
                {"detail": "No se encontraron estaciones con los criterios especificados"}, 
                status=status.HTTP_404_NOT_FOUND
            )

        # Procesar en paralelo con ThreadPoolExecutor
        resultados = []
        
        with ThreadPoolExecutor(max_workers=40) as executor:
            # Crear un diccionario de futures
            future_to_est = {
                executor.submit(
                    documentos_estaciones.get_resumen_movimientos_tanques,
                    est["Servidor"],
                    est["BaseDatos"],
                    est["Codigo"],
                    from_date,
                    until_date,
                    proveedor
                ): est
                for est in estaciones_filtradas
            }

            # Recopilar resultados a medida que se completen
            for future in as_completed(future_to_est):
                est = future_to_est[future]
                try:
                    res = future.result()
                    if res:
                        resultados.extend(res)
                except Exception as exc:
                    print(f"Error procesando estación {est['Codigo']}: {exc}")
                    # Continuar con las demás estaciones

        if not resultados:
            return Response(
                {"detail": "No se encontraron movimientos de tanques en el periodo especificado"}, 
                status=status.HTTP_404_NOT_FOUND
            )

        # Ordenar resultados por fecha y hora
        resultados_ordenados = sorted(
            resultados, 
            key=lambda x: (x.get('fecha', ''), x.get('hora_formateada', ''))
        )

        return Response(resultados_ordenados, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Error interno del servidor: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    


@api_view(['GET', 'POST'])
def get_resumen_recepciones_combustible(request):
    """
    Endpoint para obtener resumen de receopciones de combustible con información de facturas
    """
    # Obtener parámetros
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    codgas = request.data.get('codgas')
    proveedor = request.data.get('proveedor')
    company = request.data.get('company')

    # Validación de parámetros requeridos
    if not all([from_date, until_date]):
        return Response(
            {"detail": "Faltan parámetros requeridos: from y until son obligatorios"}, 
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        # Instanciar clases
        documentos_estaciones = DocumentosEstaciones()
        estacion_despachos = EstacionDespachos()
        
        # Obtener lista de estaciones
        estaciones = estacion_despachos.estaciones()
        
        # Convertir a enteros para comparaciones
        company_int = int(company or 0)
        codgas_int = int(codgas or 0)
        
        # Filtrar estaciones según criterios
        estaciones_filtradas = []
        
        if company_int == 0 and codgas_int == 0:
            # Todas las estaciones
            estaciones_filtradas = estaciones
        elif company_int == 0 and codgas_int != 0:
            # Solo la estación específica
            estaciones_filtradas = [e for e in estaciones if e["Codigo"] == codgas_int]
        elif company_int != 0 and codgas_int == 0:
            # Todas las estaciones de la empresa
            estaciones_filtradas = [e for e in estaciones if e.get("codemp") == company_int]
        else:
            # Empresa y estación específicas
            estaciones_por_empresa = [e for e in estaciones if e.get("codemp") == company_int]
            estaciones_filtradas = [e for e in estaciones_por_empresa if e["Codigo"] == codgas_int]

        if not estaciones_filtradas:
            return Response(
                {"detail": "No se encontraron estaciones con los criterios especificados"}, 
                status=status.HTTP_404_NOT_FOUND
            )

        # Procesar en paralelo con ThreadPoolExecutor
        resultados = []
        
        with ThreadPoolExecutor(max_workers=40) as executor:
            # Crear un diccionario de futures
            future_to_est = {
                executor.submit(
                    documentos_estaciones.get_resumen_recepciones_combustible,
                    est["Servidor"],
                    est["BaseDatos"],
                    est["Codigo"],
                    from_date,
                    until_date,
                    proveedor
                ): est
                for est in estaciones_filtradas
            }

            # Recopilar resultados a medida que se completen
            for future in as_completed(future_to_est):
                est = future_to_est[future]
                try:
                    res = future.result()
                    if res:
                        resultados.extend(res)
                except Exception as exc:
                    print(f"Error procesando estación {est['Codigo']}: {exc}")
                    # Continuar con las demás estaciones

        if not resultados:
            return Response(
                {"detail": "No se encontraron movimientos de tanques en el periodo especificado"}, 
                status=status.HTTP_404_NOT_FOUND
            )

        # Ordenar resultados por fecha y hora
        resultados_ordenados = sorted(
            resultados, 
            key=lambda x: (x.get('fecha', ''), x.get('hora_formateada', ''))
        )

        return Response(resultados_ordenados, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Error interno del servidor: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    



@api_view(['GET', 'POST'])
def compras_facturas_base(request):
    """
    Endpoint base para reporte de compras - parte desde FacturasRecibidas
    """
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    codgas = request.data.get('codgas', '0')
    proveedor = request.data.get('proveedor', '0')
    company = request.data.get('company', '0')
    
    from_date =  from_date + ' 00:00:01'
    until_date = until_date + ' 23:59:59'

    if not all([from_date, until_date]):
        return Response(
            {"detail": "Faltan parámetros requeridos: from y until"}, 
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        # Usar la clase modelo
        facturas_model = FacturasRecibidas()
        resultados = facturas_model.obtener_facturas_base(
            from_date=from_date,
            until_date=until_date,
            codgas=codgas,
            proveedor=proveedor,
            company=company
        )
        
        logger.info(f"Compras facturas base: {len(resultados)} registros obtenidos")
        return Response(resultados, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error en compras_facturas_base: {str(e)}", exc_info=True)
        return Response(
            {"detail": f"Error al obtener facturas: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def factura_detalle(request, factura_id):
    """
    Endpoint para obtener el detalle completo de una factura.
    """
    try:
        facturas_model = FacturasRecibidas()
        detalle = facturas_model.obtener_factura_detalle(factura_id)
        
        if not detalle:
            return Response(
                {"detail": "Factura no encontrada"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        return Response(detalle, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error en factura_detalle: {str(e)}", exc_info=True)
        return Response(
            {"detail": f"Error al obtener detalle: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
def compras_estadisticas(request):
    """
    Endpoint para obtener estadísticas de compras.
    """
    from_date = request.data.get('from')
    until_date = request.data.get('until')

    if not all([from_date, until_date]):
        return Response(
            {"detail": "Faltan parámetros requeridos: from y until"}, 
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        facturas_model = FacturasRecibidas()
        estadisticas = facturas_model.obtener_estadisticas(
            from_date=from_date,
            until_date=until_date,
            codgas=request.data.get('codgas', '0'),
            proveedor=request.data.get('proveedor', '0'),
            company=request.data.get('company', '0')
        )
        
        return Response(estadisticas, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error en compras_estadisticas: {str(e)}", exc_info=True)
        return Response(
            {"detail": f"Error al obtener estadísticas: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
@parser_classes([MultiPartParser])
def importar_factura_pdf(request):
    """
    Recibe un PDF de factura y lo importa a TG.dbo.FacturasRecibidas.

    Input (multipart/form-data):
        pdf      : archivo PDF
        proveedor: 'lobo' | 'mcg' | 'tesoro' | 'aemsa' | 'enerey' | 'essafuel' | 'premiergas' | 'petrotal'

    Output:
        {
            "estado": "exitosa" | "duplicada" | "cancelada" | "nota_credito" | "complemento_pago" | "error",
            "factura_id": int | null,
            "uuid": str,
            "proveedor": str,
            "conceptos_insertados": int,
            "mensaje": str
        }
    """
    from api.pdf_extractors.extractors import extraer_datos_pdf
    from api.pdf_extractors.conceptos import (
        extraer_conceptos_lobo, extraer_conceptos_mcg, extraer_conceptos_tesoro,
        extraer_conceptos_aemsa, extraer_conceptos_enerey, extraer_conceptos_essafuel,
        extraer_conceptos_premiergas, extraer_conceptos_petrotal,
    )

    PROVEEDORES_VALIDOS = {'lobo', 'mcg', 'tesoro', 'aemsa', 'enerey', 'essafuel', 'premiergas', 'petrotal'}
    EXTRACTORES_CONCEPTOS = {
        'lobo': extraer_conceptos_lobo,
        'mcg': extraer_conceptos_mcg,
        'tesoro': extraer_conceptos_tesoro,
        'aemsa': extraer_conceptos_aemsa,
        'enerey': extraer_conceptos_enerey,
        'essafuel': extraer_conceptos_essafuel,
        'premiergas': extraer_conceptos_premiergas,
        'petrotal': extraer_conceptos_petrotal,
    }

    pdf_file = request.FILES.get('pdf')
    proveedor = (request.data.get('proveedor') or '').strip().lower()


    if not pdf_file:
        return Response({"detail": "Falta el archivo PDF."}, status=status.HTTP_400_BAD_REQUEST)
    if proveedor not in PROVEEDORES_VALIDOS:
        return Response(
            {"detail": f"Proveedor inválido. Valores aceptados: {', '.join(sorted(PROVEEDORES_VALIDOS))}"},
            status=status.HTTP_400_BAD_REQUEST
        )

    # Guardar PDF en archivo temporal para poder pasarlo a PyMuPDF/pdfplumber
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            for chunk in pdf_file.chunks():
                tmp.write(chunk)
            tmp_path = tmp.name

        from pathlib import Path
        pdf_path = Path(tmp_path)

        # --- Extracción de datos del PDF ---
        datos = extraer_datos_pdf(pdf_path, provider_hint=proveedor)

        uuid = datos.get('UUID', '')

        # --- Filtros por tipo de documento ---
        if datos.get('__is_cancelada'):
            return Response({
                "estado": "cancelada",
                "factura_id": None,
                "uuid": uuid,
                "proveedor": proveedor,
                "conceptos_insertados": 0,
                "mensaje": "Factura cancelada — no se importa."
            }, status=status.HTTP_200_OK)

        if datos.get('__is_nota_credito'):
            return Response({
                "estado": "nota_credito",
                "factura_id": None,
                "uuid": uuid,
                "proveedor": proveedor,
                "conceptos_insertados": 0,
                "mensaje": "Nota de crédito detectada — no se importa como factura."
            }, status=status.HTTP_200_OK)

        if datos.get('__is_pago'):
            return Response({
                "estado": "complemento_pago",
                "factura_id": None,
                "uuid": uuid,
                "proveedor": proveedor,
                "conceptos_insertados": 0,
                "mensaje": "Complemento de pago detectado — no se importa."
            }, status=status.HTTP_200_OK)

        # --- Verificar duplicado por UUID ---
        importador = ImportadorFacturas()
        if uuid and importador.uuid_existe(uuid):
            return Response({
                "estado": "duplicada",
                "factura_id": None,
                "uuid": uuid,
                "proveedor": proveedor,
                "conceptos_insertados": 0,
                "mensaje": f"La factura con UUID {uuid} ya existe en la base de datos."
            }, status=status.HTTP_200_OK)

        # --- Extraer conceptos ---
        conceptos = []
        extractor_fn = EXTRACTORES_CONCEPTOS.get(proveedor)
        if extractor_fn:
            try:
                conceptos = extractor_fn(pdf_path)
            except Exception as e:
                logger.warning(f"Error extrayendo conceptos para {proveedor}: {e}")

        # --- Normalizar e insertar factura ---
        datos['RutaArchivo'] = ''
        datos['NombreArchivo'] = pdf_file.name
        datos = importador.normalizar_factura(datos)

        factura_id = importador.insertar_factura(datos)

        # --- Insertar conceptos ---
        conceptos_insertados = 0
        if factura_id and conceptos:
            conceptos_norm = [importador.normalizar_concepto(c) for c in conceptos]
            conceptos_insertados = importador.insertar_conceptos(factura_id, conceptos_norm)

        return Response({
            "estado": "exitosa",
            "factura_id": factura_id,
            "uuid": uuid,
            "proveedor": proveedor,
            "conceptos_insertados": conceptos_insertados,
            "mensaje": f"Factura importada correctamente. ID={factura_id}, {conceptos_insertados} concepto(s)."
        }, status=status.HTTP_201_CREATED)

    except Exception as e:
        logger.error(f"Error en importar_factura_pdf: {e}", exc_info=True)
        return Response(
            {"detail": f"Error al procesar el PDF: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)