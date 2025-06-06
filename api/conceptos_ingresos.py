CONCEPTOS_INGRESOS = {
    'MAXIMA': [
        'INGRESOS MAGNA',
        'INGRESOS FORANEOS MAGNA',
        'INGRESOS GASOLINA REGULAR',
        'INGRESOS MAXIMA',
        'INGRESOS MAXIMA 8%',
        'INGRESOS FORANEOS MAXIMA',
        'SOBRANTE REGULAR',
        'SOBRANTE MAGNA',
    ],
    'DIESEL': [
        'INGRESOS DIESEL',
        'INGRESOS FORANEOS DIESEL',
        'SOBRANTE DIESEL',
    ],
    'SUPER': [
        'INGRESOS PREMIUM',
        'INGRESOS GASOLINA PREMIUM',
        'INGRESOS FORANEOS PREMIUM',
        'SOBRANTE PREMIUM',
    ],
    'PREPAGO facturación': [
        'INGRESOS POR ANTICIPOS',
        'VENTAS Y/O SERV GRAV A TASA GRAL NAL PTE RELAC',
        'DEV, DESCUENTOS O BONIF SOBRE INGRESOS',
        'OTROS INGRESOS PROPIOS',
        'OTROS INGRESOS'
    ],
    'INGRESOS ACEITES Y LUBRICANTES': [
        'INGRESOS ADITIVOS',
        'INGRESOS ESPECIALES',
        'INGRESOS FORANEOS ADITIVOS',
        'INGRESOS FORANEOS ESPECIALES',
        'INGRESOS FORANEOS LUBRICANTES',
        'INGRESOS ACEITES',
        'INGRESOS LUBRICANTES',
        'INGRESOS ACEITES Y LUBRICANTES'
    ],
    'SOBRANTE REGULAR': [
        'SOBRANTE MAGNA',
        'SOBRANTE REGULAR',
        'SOBRANTES'
    ],
    'SOBRANTE PREMIUM': [
        'SOBRANTE PREMIUM',
    ],
    'SOBRANTE DIESEL': [
        'SOBRANTE DIESEL',
    ],

}

CONCEPTOS_COSTOVENTA={
    'GASOLINA REGULAR':[
        'COSTO FORANEO MAGNA',
        'COSTO MAXIMA',
        'COSTO MAGNA'
    ],
    'GASOLINA PREMIUM': [
        'COSTO PREMIUM',
        'COSTO FORANEO PREMIUM',
        'COSTO PREMIUM',
        'COSTO SUPER'
    ],
    'DIESEL': [
        'COSTO DIESEL',
        'COSTO FORANEO DIESEL',
    ],
    'MERMA GASOLINA REGULAR': [
       'Merma Magna',
       'MERMA MAXIMA'

    ],
    'MERMA GASOLINA PREMIUM': [
       'Merma Premium',
       'MERMA SUPER'
    ],
    'MERMA DIESEL':[
         'Merma Diesel'
    ],
    'ADITIVO TOTAL POWER GASOLINA REG':[
        'ADITIVO PARA GASOLINA TMAXIMA( REGULAR)',
        'ADITIVO TOTAL POWER MAGNA',
        'COSTO ADITIVO TOTAL POWER',
        'Aditivo magna',
        'COSTO ADITIVOS'

    ],
    'ADITIVO TOTAL POWER GASOLINA PREM':[
       'ADITIVO PARA GASOLINA TSUPER (PREMIUM)',
       'ADITIVO TOTAL POWER PREMIUM',
       'COSTO ADITIVO SUPER',
       'Aditivo Total Premium',
       'Aditivo Total Power Super'
    ],
    'FLETE GASOLINA REGULAR':[
        'FLETE MAGNA'
    ],
    'FLETE GASOLINA PREMIUM':[
        'FLETE PREMIUM'
    ],
    'FLETE DIESEL':[
        'FLETE DIESEL'
    ],
    'ESTIMULO REGULAR':[
        'ESTIMULO IEPS MAXIMA',
        'Estimulo IEPS Magna'
    ],
    'ESTIMULO PREMIUM':[
        'ESTIMULO IEPS SUPER',
        'Estimulo IEPS Premium'
    ],
    'DESCUENTO SOBRE VENTA':[
        'Dev, descuentos o bonif sobre ingresos',
        'Dev. Desctos, bonificaciones a clientes',
        'DESCUENTO SOBRE VENTAS',
        'DEV DESCTOS O BONIFICACIONES S/VENTAS',
        'Dev, desctos o bonifi s vtas y/o serv tasa gral',
        'Devoluciones por promociones mkt',
        'DESCUENTO SOBRE VENTA'
    ],
    'COSTO UNIFORMES':[
        'Gasto por venta de uniformes'
    ],
    'COSTO ACEITES Y LUBRICANTES':[
        'COSTO ADITIVOS'
        'COSTO ESPECIALES'
        'COSTO FORANEO ADITIVOS'
        'COSTO ACEITES'
        'COSTO FORANEO LUBRICANTES',
        'COSTO LUBRICANTES',
        'COSTO FORANEO ESPECIALES',
        'DESCUENTO SOBRE VENTA'
    ]
}
CONCEPTOS_MARGEN_UTILIDAD = {
    'REGULAR': {
        'ingresos': [
            'MAXIMA',
            'SOBRANTE REGULAR'
        ],
        'costo_venta': [
            'GASOLINA REGULAR',
            'MERMA GASOLINA REGULAR',
            'ADITIVO TOTAL POWER GASOLINA REG',
            'FLETE GASOLINA REGULAR',
            'ESTIMULO REGULAR'
        ]
    },
    'PREMIUM': {
        'ingresos': ['SUPER', 'SOBRANTE PREMIUM'],
        'costo_venta': ['GASOLINA PREMIUM', 'MERMA GASOLINA PREMIUM', 'ADITIVO TOTAL POWER GASOLINA PREMIUM', 'FLETE GASOLINA PREMIUM', 'ESTIMULO PREMIUM']
    },
    'DIESEL': {
        'ingresos': ['DIESEL', 'SOBRANTE DIESEL'],
        'costo_venta': ['DIESEL', 'MERMA DIESEL', 'FLETE DIESEL']
    },
    'OTROS CONCEPTOS': {
        'ingresos': ['PREPAGO FACTURACIÓN'],
        'costo_venta': ['DESCUENTO SOBRE VENTA', 'COSTO UNIFORMES']
    },
    'ACEITES Y LUBRICANTES': {
        'ingresos': ['INGRESOS ACEITES Y LUBRICANTES'],
        'costo_venta': ['COSTO ACEITES Y LUBRICANTES']
    },
}
