from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from concurrent.futures import ThreadPoolExecutor , as_completed
from django.http import HttpResponse
from api.modelos.estaciones_despachos import EstacionDespachos
from api.modelos.Documentos_estaciones import DocumentosEstaciones
from collections import defaultdict




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
