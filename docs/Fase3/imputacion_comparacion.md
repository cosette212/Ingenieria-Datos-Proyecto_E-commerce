# Imputacion estadistica: media vs mediana

- Ejecucion: 20261116_191713
- Outliers: criterio de Tukey con 3 x IQR (en escala logaritmica para el precio)
- Comparacion: se esconde 20 % de los valores buenos (semilla 42) y se imputan con cada estrategia; MAE = error absoluto medio, RMSE = raiz del error cuadratico medio.
- *Contaminados* = valores buenos + los que llegaron invalidos o extremos de origen. *Limpios* = solo los valores buenos.

## clean_cliente.edad

Edad del cliente (entero, rango de negocio 18-100).

| Medida | Valor |
|---|---:|
| Valores validos de origen | 487 |
| Outliers detectados | 0 |
| Valores invalidos puestos en NULL por las reglas | 7 |
| Asimetria (skew) | 0.32 |
| Media / mediana con datos limpios | 31.93 / 32.00 |
| Media / mediana con datos contaminados | 34.77 / 32.00 |
| Valores imputados | 20 (mediana = 32) |

Prueba con 97 valores escondidos (ajuste con 390):

| Estrategia | Valor usado | MAE | RMSE |
|---|---:|---:|---:|
| Media, con datos contaminados | 36.00 | 7.42 | 9.21 |
| Mediana, con datos contaminados | 32.00 | 6.47 | 7.96 |
| Media, con datos limpios | 32.00 | 6.47 | 7.96 |
| Mediana, con datos limpios | 32.00 | 6.47 | 7.96 |

Lectura:

- Con datos contaminados la media se equivoca algo mas que la mediana (MAE 7.42 contra 6.47): hay pocos valores invalidos y no pesan mucho.
- Con datos limpios, por MAE empatan (6.47 la mediana, 6.47 la media).
- Con datos limpios, por RMSE empatan (7.96 la mediana, 7.96 la media).

La media minimiza el error cuadratico (RMSE), pero un solo valor extremo la arruina; la mediana casi no se mueve. Por eso la mediana es la opcion segura.

## clean_producto.precio_lista

Precio de lista (decimal, asimetrico a la derecha).

| Medida | Valor |
|---|---:|
| Valores validos de origen | 100 |
| Outliers detectados | 3 |
| Valores invalidos puestos en NULL por las reglas | 1 |
| Asimetria (skew) | 1.48 |
| Media / mediana con datos limpios | 227.90 / 174.09 |
| Media / mediana con datos contaminados | 247,743.51 / 176.92 |
| Valores imputados | 7 (mediana = 174.09) |

Prueba con 19 valores escondidos (ajuste con 78):

| Estrategia | Valor usado | MAE | RMSE |
|---|---:|---:|---:|
| Media, con datos contaminados | 305,096.45 | 304,876.15 | 304,876.19 |
| Mediana, con datos contaminados | 185.60 | 101.23 | 144.54 |
| Media, con datos limpios | 229.76 | 112.10 | 140.63 |
| Mediana, con datos limpios | 181.09 | 100.52 | 145.69 |

Lectura:

- Con datos contaminados la media se equivoca 3,011.7 veces mas que la mediana (MAE 304,876.15 contra 101.23).
- Con datos limpios, por MAE gana la mediana (100.52 la mediana, 112.10 la media).
- Con datos limpios, por RMSE gana la media (145.69 la mediana, 140.63 la media).

La media minimiza el error cuadratico (RMSE), pero un solo valor extremo la arruina; la mediana casi no se mueve. Por eso la mediana es la opcion segura.

