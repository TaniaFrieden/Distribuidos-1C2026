# Ejercicios resueltos — MapReduce (Clase 10)

Índice:
1. [Ejercicio 1 — Log de navegaciones de una app web](#ejercicio-1--log-de-navegaciones-de-una-app-web)
   - a) Navegaciones por día de semana
   - b) URL con más navegaciones
   - c) Emails con más de 10 navegaciones
2. [Ejercicio 2 — Mensajes de un chatbot](#ejercicio-2--mensajes-de-un-chatbot)
   - a) Clientes atendidos por día del mes
   - b) Teléfonos con más de 10 mensajes en un mismo día
3. [Ejercicio 3 — Compras de moneda extranjera de un banco](#ejercicio-3--compras-de-moneda-extranjera-de-un-banco)
   - a) Monto promedio en AR$ para compra de USD, por día
   - b) Cuentas que compraron USD en 2 o más días consecutivos
   - c) Cuentas que compraron más USD que la media
4. [`emit` vs `emitIntermediate` vs `emitAll` (y `reduceAll`)](#emit-vs-emitintermediate-vs-emitall-y-reduceall)

---

# Ejercicio 1 — Log de navegaciones de una app web

Ejercicio: dado un listado de tuplas `(email, url_navegada, fecha)` de eventos de log de una aplicación web, calcular:
- a) Cantidad de navegaciones detectadas por día de semana (lu, ma, mi, ju, vi, sa, do).
- b) URL con más navegaciones registradas.
- c) Listado de emails con más de 10 navegaciones registradas.

## Supuesto general

Se asume que existe una función auxiliar `DiaDeSemana(fecha)` disponible en los Map Workers (por ejemplo, usando una librería estándar de manejo de fechas) y que el campo `fecha` de cada tupla tiene un formato del cual se puede derivar el día de la semana correspondiente.

---

## a) Cantidad de navegaciones por día de semana

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    // clave: id del chunk de log
    // valor: conjunto de tuplas (email, url, fecha)
    PARA CADA (email, url, fecha) EN valor HACER
        dia ← DiaDeSemana(fecha)        // auxiliar: fecha -> {lu, ma, mi, ju, vi, sa, do}
        Emitir(dia, 1)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: día de semana
    // listaValores: lista de "1"
    total ← Sumar(listaValores)
    Emitir(clave, total)
```

### Qué hace cada parte

**`Map`**
- El framework de MapReduce parte automáticamente el log completo en **chunks**, y ejecuta una instancia de `Map` por cada chunk (en paralelo, en distintos Map Workers). Por eso `valor` no es "todo el log", sino solo la porción que le tocó a ese worker.
- Por cada evento `(email, url, fecha)` del chunk, el `email` y la `url` **no se usan** para este cálculo — el dato relevante es solo `fecha`.
- `DiaDeSemana(fecha)` traduce una fecha concreta (ej. `"2026-07-03"`) a una categoría de día de semana (`vi`, en ese ejemplo).
- `Emitir(dia, 1)` genera un par intermedio por cada evento: la clave es el día de semana, y el valor es simplemente `1`. Es el mismo patrón que Word Count: en vez de contar palabras, se cuentan navegaciones agrupadas por día.

Al terminar la fase Map, existen muchísimos pares intermedios como:
```
(vi, 1), (lu, 1), (vi, 1), (ma, 1), (vi, 1), ...
```

**Shuffle** (paso intermedio, no está en el pseudocódigo pero lo hace la librería)

MapReduce agrupa automáticamente todos los valores que comparten la misma clave. Como acá solo hay 7 claves posibles, como máximo se generan 7 grupos, sin importar cuántos Map Workers hayan corrido:
```
lu  -> [1, 1, 1, ...]
ma  -> [1, 1, ...]
...
```

**`Reduce`**
- Se ejecuta **una vez por cada clave única** (hasta 7 veces en total), recibiendo la lista completa de "1s" que le corresponden a esa clave.
- `Sumar(listaValores)` suma esa lista de unos, dando la cantidad total de navegaciones registradas para ese día en **todo** el log (no solo en un chunk).
- `Emitir(clave, total)` produce el resultado final: el par `(día, cantidad_de_navegaciones)`.

### Resultado final

Un conjunto de hasta 7 pares, por ejemplo:
```
(lu, 1523)
(ma, 1488)
(mi, 1602)
(ju, 1550)
(vi, 2001)
(sa,  890)
(do,  770)
```

### Por qué funciona en paralelo

- La fase `Map` es completamente paralelizable: cada chunk se procesa de forma independiente, sin que un worker necesite ver los datos de otro.
- La fase `Reduce` también es paralelizable entre sí: como hay a lo sumo 7 claves, se podrían usar hasta 7 Reduce Workers simultáneos (uno por día), cada uno sumando su propia lista sin depender de los demás.
- Esto es justamente la idea central de MapReduce: identificar una función `Map` que se pueda aplicar independientemente a cada porción de datos, y una función `Reduce` que agregue los resultados agrupados por clave, sin necesitar coordinación entre distintas claves.

---

## b) URL con más navegaciones registradas

### Por qué hacen falta dos Jobs

En MapReduce, `Reduce` se invoca **una vez por cada clave**, y esa invocación solo ve los valores de *esa* clave — nunca compara contra otras claves. Por eso, con un solo Job se puede contar cuántas navegaciones tuvo cada URL (una clave por URL), pero **no** se puede en el mismo paso determinar cuál de todas esas URLs es la que tiene el máximo, porque eso requiere comparar entre claves distintas.

La solución es encadenar dos Jobs:
1. **Job 1**: cuenta navegaciones por URL (idéntico patrón a Word Count).
2. **Job 2**: toma la salida del Job 1 y la reduce a una única clave para poder comparar todos los conteos entre sí y quedarse con el máximo.

### Pseudocódigo — Job 1 (conteo por URL)

```
FUNCIÓN Map(clave, valor):
    PARA CADA (email, url, fecha) EN valor HACER
        Emitir(url, 1)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: url
    total ← Sumar(listaValores)
    Emitir(clave, total)     // (url, cantidad)
```

- `Map`: por cada evento, ignora `email` y `fecha`, y emite `(url, 1)`.
- Shuffle: agrupa todos los `1` por cada URL distinta.
- `Reduce`: se ejecuta una vez por URL, suma sus "1s" y devuelve `(url, cantidad_total_de_navegaciones)`.

Salida del Job 1, por ejemplo:
```
(/home,     5000)
(/login,    3200)
(/producto, 7800)
(/carrito,  1200)
...
```

### Pseudocódigo — Job 2 (máximo global)

```
FUNCIÓN Map(clave, valor):
    // clave: url, valor: cantidad (salida del Job 1)
    Emitir("max", (clave, valor))    // clave constante -> un único Reduce Worker

FUNCIÓN Reduce(clave, listaValores):
    // listaValores: lista de pares (url, cantidad)
    mejorUrl ← NULO
    mejorCantidad ← -1
    PARA CADA (url, cantidad) EN listaValores HACER
        SI cantidad > mejorCantidad ENTONCES
            mejorUrl ← url
            mejorCantidad ← cantidad
        FIN SI
    FIN PARA
    Emitir(mejorUrl, mejorCantidad)
```

- `Map`: toma cada par `(url, cantidad)` de la salida del Job 1 y lo re-emite bajo una **clave constante** (`"max"`). Al usar siempre la misma clave, el Shuffle agrupa *todos* los pares `(url, cantidad)` juntos, sin importar cuántas URLs distintas haya.
- Shuffle: como hay una sola clave, se forma un único grupo con todos los `(url, cantidad)`. Esto fuerza a que un solo Reduce Worker procese todo — es la única forma de poder comparar entre sí conteos que antes vivían en claves (URLs) separadas.
- `Reduce`: recibe la lista completa de `(url, cantidad)` y hace un simple barrido lineal quedándose con el máximo, que es el resultado final.

### Ejemplo paso a paso

Log de entrada, partido en 2 chunks (2 Map Workers), con 8 eventos en total:

**Chunk 1** (Map Worker 1):
```
(ana@mail.com,  /home,     2026-07-01)
(juan@mail.com, /producto, 2026-07-01)
(ana@mail.com,  /producto, 2026-07-02)
(sofi@mail.com, /home,     2026-07-02)
```

**Chunk 2** (Map Worker 2):
```
(juan@mail.com, /producto, 2026-07-03)
(sofi@mail.com, /carrito,  2026-07-03)
(ana@mail.com,  /home,     2026-07-04)
(juan@mail.com, /home,     2026-07-04)
```

**Job 1 — Paso 1: `Map`** (cada worker emite `(url, 1)`, en paralelo):
```
Map Worker 1 emite:            Map Worker 2 emite:
(/home,     1)                 (/producto, 1)
(/producto, 1)                 (/carrito,  1)
(/producto, 1)                 (/home,     1)
(/home,     1)                 (/home,     1)
```

**Job 1 — Paso 2: Shuffle** (agrupa por `url`):
```
/home     -> [1, 1, 1, 1]
/producto -> [1, 1, 1]
/carrito  -> [1]
```

**Job 1 — Paso 3: `Reduce`** (una invocación por URL, en paralelo):
```
Reduce(/home,     [1,1,1,1]) -> Sumar = 4 -> Emitir(/home,     4)
Reduce(/producto, [1,1,1])   -> Sumar = 3 -> Emitir(/producto, 3)
Reduce(/carrito,  [1])       -> Sumar = 1 -> Emitir(/carrito,  1)
```

**Salida del Job 1:**
```
(/home,     4)
(/producto, 3)
(/carrito,  1)
```

**Job 2 — Paso 1: `Map`** (re-emite todo con clave constante `"max"`):
```
Emitir("max", (/home,     4))
Emitir("max", (/producto, 3))
Emitir("max", (/carrito,  1))
```

**Job 2 — Paso 2: Shuffle** (una sola clave, un solo grupo):
```
"max" -> [(/home, 4), (/producto, 3), (/carrito, 1)]
```

**Job 2 — Paso 3: `Reduce`** (una sola invocación, un solo worker):
```
mejorUrl, mejorCantidad = NULO, -1

(/home, 4):     4 > -1  -> mejorUrl=/home,     mejorCantidad=4
(/producto, 3): 3 > 4?  -> NO, no cambia
(/carrito, 1):  1 > 4?  -> NO, no cambia

Emitir(/home, 4)
```

**Salida del Job 2 (resultado final del ejercicio):**
```
(/home, 4)
```

`/home` es la URL con más navegaciones registradas en todo el log (4 de las 8 navegaciones totales).

### Supuesto clave

Se asume que la cantidad de URLs distintas es mucho menor que la cantidad total de eventos del log, por lo que concentrar todos los pares `(url, cantidad)` en un solo Reduce Worker en el Job 2 es viable en memoria (no sería viable si en cambio se intentara concentrar así los eventos crudos del log completo).

---

## c) Emails con más de 10 navegaciones

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    PARA CADA (email, url, fecha) EN valor HACER
        Emitir(email, 1)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: email
    cantidad ← Sumar(listaValores)
    SI cantidad > 10 ENTONCES
        Emitir(clave, cantidad)
    FIN SI
```

### Qué hace cada parte

- `Map`: por cada evento del chunk, ignora `url` y `fecha`, y emite `(email, 1)`. Mismo patrón que Word Count y que el ítem a), cambiando qué campo se usa como clave.
- Shuffle: agrupa todos los `1` por cada email distinto, formando una clave por cada usuario que aparece en el log.
- `Reduce`: se ejecuta una vez por email, suma sus "1s" para obtener el total de navegaciones de ese usuario, y **filtra ahí mismo**: solo emite el resultado si `cantidad > 10`.

A diferencia del ítem b), acá **no hace falta un segundo Job**: el criterio "más de 10 navegaciones" es una condición que se puede evaluar con la información de una sola clave (un email a la vez), sin necesitar compararla contra otros emails.

### Resultado final

Un listado con únicamente los emails que superan las 10 navegaciones, por ejemplo:
```
(ana@mail.com,   15)
(juan@mail.com,  23)
(sofi@mail.com,  11)
```

Los emails con 10 navegaciones o menos simplemente no emiten nada en su `Reduce`, y no aparecen en la salida final.

---

# Ejercicio 2 — Mensajes de un chatbot

Ejercicio: dado un listado de tuplas `(messageId, clientPhoneNumber, text, date, time)` de eventos de mensajes recibidos en un chatbot, calcular:
- a) Cantidad de clientes atendidos por día del mes (1, 2, ..., 30 o 31).
- b) Listado de teléfonos con más de 10 mensajes recibidos en un mismo día.

## Supuestos

- `date` tiene un formato del que se puede derivar el día del mes; se asume una función auxiliar `DiaDelMes(date)` que devuelve un número entre 1 y 30/31.
- `time`, `text` y `messageId` no se usan en ninguno de los dos cálculos.
- "Clientes atendidos" en a) se interpreta como **clientes distintos** (por `clientPhoneNumber`) que enviaron al menos un mensaje ese día — si un mismo cliente manda varios mensajes el mismo día, cuenta una sola vez.
- En b), "en un mismo día" indica que el conteo de mensajes es **por teléfono y por día** (no un acumulado de todo el mes). Si un teléfono supera los 10 mensajes en más de un día, se lo lista una vez por cada día en que lo supera.

---

## a) Cantidad de clientes atendidos por día del mes

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    // clave: id del chunk de log
    // valor: conjunto de tuplas (messageId, clientPhoneNumber, text, date, time)
    PARA CADA (messageId, clientPhoneNumber, text, date, time) EN valor HACER
        dia ← DiaDelMes(date)      // auxiliar: fecha -> {1, 2, ..., 30 o 31}
        Emitir(dia, clientPhoneNumber)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: día del mes
    // listaValores: lista de teléfonos (puede tener repetidos)
    clientesUnicos ← ConjuntoUnico(listaValores)
    Emitir(clave, len(clientesUnicos))
```

### Qué hace cada parte

- `Map`: por cada mensaje del chunk, ignora `messageId`, `text` y `time`, y emite `(dia, clientPhoneNumber)` — el valor emitido no es un contador fijo como `1`, sino el propio teléfono, porque lo que se necesita contar son **clientes distintos**, no mensajes.
- Shuffle: agrupa por día del mes (a lo sumo 31 claves), formando por cada día la lista completa de teléfonos que escribieron ese día (con repeticiones si un cliente mandó varios mensajes).
- `Reduce`: se ejecuta una vez por día. `ConjuntoUnico(listaValores)` deduplica los teléfonos repetidos, y `len(...)` cuenta cuántos clientes distintos hay en ese conjunto. Esa deduplicación es la diferencia clave frente a un Word-Count común: acá no alcanza con sumar, hay que sacar duplicados antes de contar.

### Ejemplo paso a paso

Log de entrada, partido en 2 chunks:

**Chunk 1:**
```
(m1, +5491111111111, "hola",    2026-07-01, 10:00)
(m2, +5491122222222, "precio?", 2026-07-01, 10:05)
(m3, +5491111111111, "gracias", 2026-07-01, 11:00)
(m4, +5491133333333, "hola",    2026-07-02, 09:00)
```

**Chunk 2:**
```
(m5, +5491122222222, "hola",    2026-07-01, 12:00)
(m6, +5491144444444, "hola",    2026-07-02, 08:30)
(m7, +5491133333333, "info",    2026-07-02, 09:15)
```

**Paso 1: `Map`** (en paralelo):
```
Map Worker 1 emite:                    Map Worker 2 emite:
(1, +5491111111111)                    (1, +5491122222222)
(1, +5491122222222)                    (2, +5491144444444)
(1, +5491111111111)                    (2, +5491133333333)
(2, +5491133333333)
```

**Paso 2: Shuffle:**
```
1 -> [+5491111111111, +5491122222222, +5491111111111, +5491122222222]
2 -> [+5491133333333, +5491144444444, +5491133333333]
```

**Paso 3: `Reduce`:**
```
Reduce(1, [...]) -> ConjuntoUnico = {+5491111111111, +5491122222222} -> len = 2 -> Emitir(1, 2)
Reduce(2, [...]) -> ConjuntoUnico = {+5491133333333, +5491144444444} -> len = 2 -> Emitir(2, 2)
```

**Resultado final:**
```
(1, 2)   // el día 1 del mes se atendieron 2 clientes distintos
(2, 2)   // el día 2 del mes se atendieron 2 clientes distintos
```

Nótese que aunque hubo 7 mensajes en total y 4 de ellos ocurrieron el día 1, solo 2 clientes distintos escribieron ese día (`+5491111111111` escribió 2 veces y `+5491122222222` escribió 2 veces) — por eso hace falta `ConjuntoUnico` antes de contar.

### Alternativa sin `ConjuntoUnico`: deduplicar con la clave, en 2 Jobs

La versión de arriba deduplica **dentro del `Reduce`**, con una función auxiliar `ConjuntoUnico` que debe tener en memoria toda la lista de teléfonos repetidos de un día antes de poder contar. Existe una alternativa más "MapReduce-idiomática": en vez de deduplicar a mano, aprovechar que **`Reduce(clave, listaValores)` se invoca una única vez por cada clave única** — esa garantía del framework *es* en sí misma un mecanismo de deduplicación, si se agrupa por la clave correcta.

Para eso hace falta encadenar dos Jobs, igual que en el ítem b) de las URLs:

**Job 1 — obtener pares únicos (día, teléfono):**
```
FUNCIÓN Map(clave, valor):
    PARA CADA (messageId, clientPhoneNumber, text, date, time) EN valor HACER
        dia ← DiaDelMes(date)
        Emitir((dia, clientPhoneNumber), 1)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: (dia, telefono) — se llama UNA sola vez por cada par único,
    // sin importar cuántos mensajes mandó ese cliente ese día
    Emitir(clave, 1)     // (dia, telefono) aparece exactamente una vez en la salida
```

**Job 2 — contar clientes distintos por día:**
```
FUNCIÓN Map(clave, valor):
    // clave: (dia, telefono), valor: 1  (salida del Job 1)
    (dia, telefono) ← clave
    Emitir(dia, 1)

FUNCIÓN Reduce(clave, listaValores):
    // clave: día del mes
    total ← Sumar(listaValores)
    Emitir(clave, total)
```

Acá la dedup no la hace una función auxiliar operando sobre una lista en memoria: la hace la propia mecánica de MapReduce. En el Job 1, sin importar cuántas veces el mismo teléfono escribió ese día, `Reduce((dia, telefono), ...)` se ejecuta una sola vez y emite un solo `(dia, telefono)`. El Job 2 ya solo tiene que sumar esos pares por día.

**¿Por qué no sirve `emitAll`/`reduceAll` acá?** Esas primitivas calculan **un único escalar global**, igual en todas las invocaciones de `Reduce` (ej. "cuántos clientes distintos hubo en *todo* el mes"). Lo que pide el enunciado es un desglose **por día** (31 resultados distintos), no un solo número global — por eso no encajan como reemplazo de `ConjuntoUnico`.

**Comparación de las dos versiones:**

| | Con `ConjuntoUnico` (1 Job) | Con clave compuesta (2 Jobs) |
|---|---|---|
| Dedup | Explícita, dentro del `Reduce`, sobre una lista que puede tener muchos repetidos | Implícita, aprovechando que `Reduce` se invoca una vez por clave única |
| Memoria | El `Reduce` de un día muy activo debe tener en memoria **todos** los teléfonos repetidos de ese día antes de deduplicar | Cada `Reduce` del Job 1 recibe solo los repetidos de un `(dia, telefono)` puntual — mucho más chico |
| Cantidad de Jobs | 1 | 2 |

Ambas son válidas para este ejercicio; la de clave compuesta es la que se usa en la práctica real (Hadoop, Spark) cuando los grupos por clave simple serían demasiado grandes para deduplicar en memoria de una sola vez.

---

## b) Teléfonos con más de 10 mensajes en un mismo día

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    PARA CADA (messageId, clientPhoneNumber, text, date, time) EN valor HACER
        dia ← DiaDelMes(date)
        Emitir((clientPhoneNumber, dia), 1)     // clave compuesta: (teléfono, día)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: (teléfono, día)
    cantidad ← Sumar(listaValores)
    SI cantidad > 10 ENTONCES
        Emitir(clave, cantidad)
    FIN SI
```

### Qué hace cada parte

- `Map`: emite una clave **compuesta** `(clientPhoneNumber, dia)`, no solo el teléfono — porque el umbral de "más de 10" aplica dentro de un mismo día, no acumulado en todo el mes. Si se usara solo el teléfono como clave, se estaría contando el total de mensajes del mes completo, perdiendo la restricción "en un mismo día".
- Shuffle: agrupa por esa clave compuesta, de forma que cada grupo contiene únicamente los mensajes de un teléfono en un día particular.
- `Reduce`: suma la cantidad de mensajes de ese grupo y filtra directamente ahí — igual que en el ejercicio de "emails con más de 10 navegaciones", el filtro es local a la clave, así que no hace falta un segundo Job.

### Ejemplo paso a paso

Log de entrada, un solo chunk (para simplificar), con el teléfono `+5491111111111` escribiendo 12 veces el día 5 y 3 veces el día 6, y otro teléfono `+5491122222222` escribiendo 4 veces el día 5:
```
12 mensajes de +5491111111111 el día 5
 3 mensajes de +5491111111111 el día 6
 4 mensajes de +5491122222222 el día 5
```

**Paso 1: `Map`:**
```
((+5491111111111, 5), 1)  x12
((+5491111111111, 6), 1)  x3
((+5491122222222, 5), 1)  x4
```

**Paso 2: Shuffle:**
```
(+5491111111111, 5) -> [1,1,1,1,1,1,1,1,1,1,1,1]   (12 unos)
(+5491111111111, 6) -> [1,1,1]                      (3 unos)
(+5491122222222, 5) -> [1,1,1,1]                    (4 unos)
```

**Paso 3: `Reduce`:**
```
Reduce((+5491111111111, 5), [12 unos]) -> cantidad=12 -> 12>10 -> Emitir((+5491111111111, 5), 12)
Reduce((+5491111111111, 6), [3 unos])  -> cantidad=3  -> 3>10?  NO -> no emite nada
Reduce((+5491122222222, 5), [4 unos])  -> cantidad=4  -> 4>10?  NO -> no emite nada
```

**Resultado final:**
```
((+5491111111111, 5), 12)
```

Solo aparece el teléfono `+5491111111111` para el día 5, donde superó el umbral de 10 mensajes. El mismo teléfono no aparece para el día 6 (solo tuvo 3 mensajes ese día), y el otro teléfono tampoco aparece (tuvo 4 mensajes el día 5). Si `+5491111111111` también hubiese superado el umbral en otro día, aparecería una segunda vez en la salida, con ese otro día como parte de la clave.

---

# Ejercicio 3 — Compras de moneda extranjera de un banco

Ejercicio: dado un listado de tuplas `(fecha, num-cuenta, cod-divisa, cantidad-divisas, tasa-cambio-AR$)` con todas las compras de moneda extranjera de un banco a lo largo del mes, calcular:
- a) Monto promedio de AR$ utilizado para compra de divisas "USD", por día del mes.
- b) Listado de números de cuenta que compraron divisas "USD" durante 2 o más días consecutivos.
- c) Listado de números de cuenta que compraron más divisas "USD" que la media.

## Supuestos

- `fecha` tiene un formato del que se puede derivar el día del mes; se usa la misma función auxiliar `DiaMes(fecha)` de los ejercicios anteriores.
- El **monto en AR$** de una compra puntual se calcula como `cantidad-divisas * tasa-cambio-AR$` (cantidad de moneda extranjera comprada, multiplicada por su cotización en pesos).
- Los tres incisos filtran únicamente las compras donde `cod-divisa == "USD"` — las compras de otras divisas (si las hubiera en el log) se descartan.
- En c), "la media" se calcula sobre el total de USD comprado **por cada cuenta que compró USD al menos una vez** (no sobre todas las cuentas del banco, ya que una cuenta que nunca compró USD no tiene un "monto comprado" definido).

---

## a) Monto promedio de AR$ para compra de USD, por día

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    // valor: conjunto de tuplas (fecha, num-cuenta, cod-divisa, cantidad-divisas, tasaCambio)
    PARA CADA (fecha, numCuenta, codDivisa, cantidadDivisas, tasaCambio) EN valor HACER
        SI codDivisa == "USD" ENTONCES
            dia ← DiaMes(fecha)
            montoARS ← cantidadDivisas * tasaCambio
            Emitir(dia, montoARS)
        FIN SI
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: día del mes
    // listaValores: lista de montos en AR$ de las compras de USD de ese día
    promedio ← Sumar(listaValores) / len(listaValores)
    Emitir(clave, promedio)
```

### Qué hace cada parte

- `Map`: descarta cualquier compra que no sea de USD con el `SI codDivisa == "USD"`. Para las que sí son de USD, calcula el monto en pesos de esa compra puntual (`cantidadDivisas * tasaCambio`) y lo emite agrupado por día — `num-cuenta` no se usa en este cálculo.
- Shuffle: agrupa por día del mes (a lo sumo 31 claves), como en los ejercicios anteriores.
- `Reduce`: recibe todos los montos en AR$ de compras de USD de un día particular, y calcula el promedio dividiendo la suma por la cantidad de compras de ese día. Es el mismo patrón que Word Count/navegaciones por día, pero promediando en vez de sumar.

### Resultado final

Un conjunto de hasta 31 pares, por ejemplo:
```
(1,  185000.50)
(2,  201340.00)
(3,  178900.25)
...
```

---

## b) Cuentas que compraron USD en 2 o más días consecutivos

### Por qué alcanza con un solo Job

A diferencia del Ejercicio 1b) (URL con más navegaciones), acá **no** hace falta comparar entre distintas cuentas — el criterio "2 o más días consecutivos" se puede evaluar mirando únicamente los días en que compró USD *esa misma cuenta*, sin necesitar los datos de las demás. Es una condición local a cada clave, como en el filtro de "más de 10" de los ejercicios anteriores.

### Pseudocódigo

```
FUNCIÓN Map(clave, valor):
    PARA CADA (fecha, numCuenta, codDivisa, cantidadDivisas, tasaCambio) EN valor HACER
        SI codDivisa == "USD" ENTONCES
            dia ← DiaMes(fecha)
            Emitir(numCuenta, dia)
        FIN SI
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: num-cuenta
    // listaValores: lista de días en que compró USD (puede tener repetidos)
    diasOrdenados ← Ordenar(ConjuntoUnico(listaValores))
    SI ExisteParConsecutivo(diasOrdenados) ENTONCES
        Emitir(clave, clave)      // solo interesa el número de cuenta, no los días
    FIN SI

FUNCIÓN ExisteParConsecutivo(diasOrdenados):
    // función auxiliar: recorre la lista ya ordenada y sin repetidos
    PARA i DESDE 0 HASTA len(diasOrdenados) - 2 HACER
        SI diasOrdenados[i+1] - diasOrdenados[i] == 1 ENTONCES
            RETORNAR verdadero
        FIN SI
    FIN PARA
    RETORNAR falso
```

### Qué hace cada parte

- `Map`: filtra solo compras de USD y emite `(numCuenta, dia)` — como en el Ejercicio 2a), el valor emitido es un dato (el día), no un contador.
- Shuffle: agrupa por número de cuenta, formando la lista completa de días en que esa cuenta compró USD durante el mes (con repetidos si compró más de una vez el mismo día).
- `Reduce`: primero deduplica y ordena los días (`ConjuntoUnico` + `Ordenar`), y después recorre la lista ordenada buscando dos días adyacentes cuya diferencia sea exactamente 1 — eso es lo que significa "consecutivos". Si encuentra al menos un par así, emite **solo el número de cuenta** (`Emitir(clave, clave)`, el mismo patrón que el ejemplo de `Union` del resumen) — el enunciado pide un listado de cuentas, no el detalle de qué días compró.

### Ejemplo paso a paso

Para la cuenta `1001`, el log tiene compras de USD en los días `5, 12, 13, 20` (algunos repetidos):
```
Map emite: (1001, 5), (1001, 12), (1001, 12), (1001, 13), (1001, 20)
```
Shuffle agrupa:
```
1001 -> [5, 12, 12, 13, 20]
```
Reduce:
```
diasOrdenados = ConjuntoUnico([5,12,12,13,20]) ordenado = [5, 12, 13, 20]
ExisteParConsecutivo([5,12,13,20]):
    12 - 5  = 7   -> no
    13 - 12 = 1   -> SÍ, hay un par consecutivo (12 y 13)
Emitir(1001, 1001)
```

**Resultado final (solo números de cuenta):**
```
1001
```
La cuenta `1001` aparece en la salida porque compró USD dos días seguidos (12 y 13), aunque el resto de sus compras (día 5 y día 20) no sean consecutivas entre sí. La salida es simplemente el listado de cuentas que cumplen la condición — no se informan los días.

---

## c) Cuentas que compraron más USD que la media

### Por qué hacen falta dos Jobs

"La media" es un valor que depende de **todas** las cuentas a la vez (es un promedio global), pero para calcularla primero hay que saber cuánto compró cada cuenta — un resultado que solo existe después de un `Reduce` por cuenta. Es la misma situación que en el Ejercicio 1b): un agregado global que depende de datos ya agrupados por clave necesita un segundo Job encadenado.

### Pseudocódigo — Job 1 (total de USD comprado por cuenta)

```
FUNCIÓN Map(clave, valor):
    PARA CADA (fecha, numCuenta, codDivisa, cantidadDivisas, tasaCambio) EN valor HACER
        SI codDivisa == "USD" ENTONCES
            Emitir(numCuenta, cantidadDivisas)
        FIN SI
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: num-cuenta
    total ← Sumar(listaValores)
    Emitir(clave, total)     // (num-cuenta, total-usd-comprado)
```

### Pseudocódigo — Job 2 (comparar contra la media, usando `emitAll`/`reduceAll`)

```
FUNCIÓN Map(clave, valor):
    // clave: num-cuenta, valor: total (salida del Job 1)
    emitAll("", valor)                  // aporta al cálculo de la media global
    emitIntermediate(clave, valor)      // sigue el flujo normal, sin cambios

FUNCIÓN Reduce(clave, listaValores):
    // clave: num-cuenta, listaValores: [total] (un solo elemento, viene del Job 1)
    media ← reduceAll("", promedio)
    total ← listaValores[0]
    SI total > media ENTONCES
        Emitir(clave, total)
    FIN SI
```

### Qué hace cada parte

- Job 1 es un Word-Count clásico: suma el total de USD comprado por cada cuenta.
- Job 2 usa `emitAll("", valor)` en el `Map` para que **cada** total de cuenta aporte al cálculo de la media global, en paralelo a que ese mismo par siga su camino normal con `emitIntermediate`.
- En el `Reduce`, `reduceAll("", promedio)` calcula la media de todos los totales (visible por igual en todas las invocaciones de `Reduce`), y cada cuenta simplemente compara **su propio** total contra esa media — sin necesitar ver los totales de las demás cuentas directamente.

Esto es un buen ejemplo de cuándo `emitAll`/`reduceAll` sí resuelven un Job completo por sí solos: acá no hace falta identificar una única cuenta "ganadora" comparando pares entre sí (como si fuera un máximo), sino comparar cada clave contra un **agregado ya calculado** (la media) — un caso perfecto para `reduceAll`.

> Esta misma técnica también podría haberse usado en el Ejercicio 1b) para el Job 2 (en vez de la clave constante `"max"` + barrido manual): con `emitAll("", cantidad)` + `reduceAll("", max)`, cada URL podría comparar su propio conteo contra el máximo global y emitirse solo si coincide. Ver la nota al final de la sección de `emitAll`/`reduceAll`, más abajo.

### Ejemplo paso a paso

Salida del Job 1 (3 cuentas):
```
(1001, 500)
(1002, 900)
(1003, 100)
```

**Job 2 — Map:**
```
emitAll("", 500)          emitIntermediate(1001, 500)
emitAll("", 900)          emitIntermediate(1002, 900)
emitAll("", 100)          emitIntermediate(1003, 100)
```

**Job 2 — cálculo de la media global (vía `reduceAll`):**
```
media = promedio([500, 900, 100]) = 1500 / 3 = 500
```

**Job 2 — Reduce (una invocación por cuenta):**
```
Reduce(1001, [500]) -> 500 > 500? NO -> no emite nada
Reduce(1002, [900]) -> 900 > 500? SÍ -> Emitir(1002, 900)
Reduce(1003, [100]) -> 100 > 500? NO -> no emite nada
```

**Resultado final:**
```
(1002, 900)
```

Solo la cuenta `1002` compró más USD que la media (500); las cuentas `1001` (exactamente en la media) y `1003` (por debajo) no aparecen en la salida.

---

# `emit` vs `emitIntermediate` vs `emitAll` (y `reduceAll`)

## Fase Map

| Primitiva | Para qué sirve |
|---|---|
| **`emitIntermediate(clave, valor)`** | Es el canal "normal" Map → Reduce. Cada par que emite pasa por el **Shuffle** (se agrupa por `clave`) y termina siendo un elemento de la `listaValores` que recibe `Reduce(clave, listaValores)` para *esa* clave. Es lo que se usa en prácticamente todo (Word Count, y los ejercicios 1 y 2 de más arriba). |
| **`emitAll(clave, valor)`** | Manda un valor a un **canal global**, en paralelo al flujo normal. No se agrupa por clave para ir a un `Reduce` particular — se acumula entre *todas* las invocaciones de Map, usando una clave global (normalmente una clave dummy como `""`), para ser consumido después con `reduceAll`. |

## Fase Reduce

| Primitiva | Para qué sirve |
|---|---|
| **`emit(clave, valor)`** | Escribe el **resultado final** (output file) de esa invocación de `Reduce`. No pasa por ningún agrupamiento posterior — es lo último que se produce. |
| **`reduceAll(clave, agregador)`** | Lee **todos** los valores que se mandaron con `emitAll` bajo esa clave global, y les aplica una función de agregación (`sum`, `(sort, uniq, len)`, etc.), devolviendo un escalar. Ese escalar está disponible **dentro de cada invocación de `Reduce`**, no solo en una — es la forma de que un cálculo "por clave" pueda usar una estadística de *todo* el dataset. |

## Ejemplos de uso

### `emitIntermediate` + `emit` — Word Count (conteo simple por clave)

```
FUNCIÓN Map(clave, valor):
    // valor: documento como texto multilínea
    PARA CADA palabra w EN valor HACER
        emitIntermediate(w, 1)
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    // clave: palabra, listaValores: lista de "1"
    emit(clave, len(listaValores))
```
`emitIntermediate` manda cada ocurrencia de palabra al Shuffle para agruparla; `emit` en el Reduce escribe el conteo final `(palabra, cantidad)`. Es exactamente el mismo patrón que se usó en el Ejercicio 1a) (agrupando por día), 1b)-Job1 (agrupando por URL), 1c) (agrupando por email) y el Ejercicio 2 (agrupando por día del mes o por teléfono+día).

### `emitAll` + `reduceAll` — Word Frequency (necesita un total global)

```
FUNCIÓN Map(clave, valor):
    PARA CADA palabra w EN valor HACER
        emitAll("", 1)              // aporta al total global de palabras
        emitIntermediate(w, 1)      // sigue yendo agrupado por palabra, como siempre
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    totalPalabras ← reduceAll("", sum)      // escalar global, igual en todas las invocaciones
    emit(clave, len(listaValores) / totalPalabras)
```
Acá cada `Reduce(palabra, ...)` necesita dividir por el total de palabras del documento entero — un dato que no está contenido en su propia `listaValores` (que solo tiene las ocurrencias de *esa* palabra). `emitAll("", 1)` en el Map acumula un `1` por cada palabra procesada, sin importar cuál sea; `reduceAll("", sum)` suma todos esos "1" y da el total global, visible desde cualquier invocación de Reduce.

### `emitAll` + `reduceAll` — Intersect (necesita saber "cuántos documentos hay en total")

```
FUNCIÓN Map(clave, valor):
    // clave: nombre del documento
    diccionario ← {}
    emitAll("", clave)                          // aporta el nombre de este documento al total
    PARA CADA palabra w EN valor HACER
        SI w NO ESTÁ EN diccionario ENTONCES
            diccionario[w] ← clave
            emitIntermediate(w, clave)
        FIN SI
    FIN PARA

FUNCIÓN Reduce(clave, listaValores):
    cantidadDocumentos ← reduceAll("", (ordenar, unicos, len))
    documentosVistos ← ConjuntoUnico(listaValores)
    SI len(documentosVistos) == cantidadDocumentos ENTONCES
        emit(clave, clave)
    FIN SI
```
`emitAll("", clave)` manda el nombre de cada documento (una vez por Map, es decir, una vez por documento) a la clave global. `reduceAll("", (ordenar, unicos, len))` ordena, deduplica y cuenta esos nombres, dando la **cantidad total de documentos distintos** — dato que ninguna `Reduce(palabra, ...)` podría calcular mirando solo su propia `listaValores` (que solo tiene los documentos donde apareció *esa* palabra). Con ese total, cada palabra puede chequear si apareció en *todos* los documentos.

## Por qué en el Ejercicio 1b) se usó una clave constante en vez de `emitAll`/`reduceAll` (y cuándo sí conviene usarlas)

`emitAll` se llama **desde `Map`**, sobre los datos que le llegan a esa fase — por eso sirve para agregados que se calculan directamente sobre esos datos, sin necesitar agruparlos por clave primero (contar filas totales, contar documentos totales, promediar valores). En el Ejercicio 1b) resolvimos el Job 2 con una clave constante (`"max"`) y un barrido manual dentro del `Reduce`.

Esa **no** es la única forma: `emitAll`/`reduceAll` también podrían haber resuelto ese mismo Job 2, de forma más prolija:

```
FUNCIÓN Map(clave, valor):
    // clave: url, valor: cantidad (salida del Job 1)
    emitAll("", valor)                  // aporta al cálculo del máximo global
    emitIntermediate(clave, valor)      // sigue el flujo normal, sin cambios

FUNCIÓN Reduce(clave, listaValores):
    // clave: url, listaValores: [cantidad] (un solo elemento, viene del Job 1)
    maxGlobal ← reduceAll("", max)
    SI listaValores[0] == maxGlobal ENTONCES
        emit(clave, listaValores[0])
    FIN SI
```

Esto funciona porque `reduceAll("", max)` calcula el máximo global una sola vez y lo deja disponible en **todas** las invocaciones de `Reduce` — cada URL simplemente chequea si su propio conteo coincide con ese máximo. Es exactamente el mismo principio usado en el Ejercicio 3c) (comparar cada clave contra un agregado global calculado con `reduceAll`), solo que ahí el agregador es `promedio` en vez de `max`.

Lo que sí sigue siendo necesario en ambos casos es un **segundo Job**: el agregado que se necesita (máximo, promedio, etc.) depende de datos que ya pasaron por un `Reduce` previo (el conteo por URL, o el total de USD por cuenta), y `emitAll`/`reduceAll` operan sobre los datos que le llegan a *esa* fase de Map — no pueden "saltar hacia atrás" a datos de un Job anterior antes de que ese Job haya terminado de correr.
