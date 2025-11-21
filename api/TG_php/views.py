from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from concurrent.futures import ThreadPoolExecutor , as_completed
from django.http import HttpResponse
from api.modelos.estaciones_despachos import EstacionDespachos
from api.modelos.Documentos_estaciones import DocumentosEstaciones
from api.modelos.inventarios_estaciones import InventariosEstaciones

from collections import defaultdict
import json



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

    # estaciones_filtradas = estaciones if int(codgas)==0 else [e for e in estaciones if e["Codigo"]==int(codgas)]
    # resultados = documentos_estaciones.get_purchase_from_station(
    #     estaciones_filtradas[0]["Servidor"],
    #     estaciones_filtradas[0]["BaseDatos"],
    #     estaciones_filtradas[0]["Codigo"],
    #     from_date,
    #     until_date
    # )
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