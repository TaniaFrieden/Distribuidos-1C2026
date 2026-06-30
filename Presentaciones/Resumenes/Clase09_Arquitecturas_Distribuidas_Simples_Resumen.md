# Sistemas Distribuidos I (75.74) — Clase 09: Arquitecturas Distribuidas Simples

## 1. Cliente-Servidor

- Se definen roles para los participantes:
  - **Servidor** como elemento **pasivo** que provee servicios.
  - **Clientes activos** que envían pedidos al servidor.
- Permite **centralización** en la toma de decisiones.
- Suele asumirse que los servidores tienen más capacidades de hardware que los clientes.

![Modelo Cliente-Servidor simple: un Server atendiendo a múltiples Clients](Clase09_imagenes/pag-03.png)

También puede organizarse de forma **jerárquica**, con servidores intermedios que a su vez actúan como clientes de un servidor superior:

![Cliente-Servidor jerárquico: Server raíz con Servers intermedios atendiendo Clients](Clase09_imagenes/pag-04.png)

### Flujos de Comunicación

![Diagrama de secuencia: Client 1 envía mensaje al Server, que lo reenvía a Client 2](Clase09_imagenes/pag-05.png)

- Los clientes deben conocer la **ubicación del servidor** para poder utilizarlo.
- Los clientes **no entablan comunicaciones entre sí**, salvo a través del servidor (ej. `msg-for-2` de Client 1 al Server, que el Server reenvía a Client 2 como `msg-from-1`).
- Se pueden utilizar modelos de **callback** aunque no es su carácter natural:
  - **Long polling**.
  - **Push notifications**.

---

## 2. Peer-to-Peer

![Red de Peers interconectados entre sí](Clase09_imagenes/pag-08.png)

- Se establece una red de nodos que se consideran **pares (peers)** entre sí.
- Asume capacidades de recursos **similares** entre los pares.
- Muy útil cuando existen objetivos de **colaboración** por parte del negocio:
  - Requiere un **protocolo acordado** entre las partes.
  - La lógica distribuida requiere **coherencia** entre los nodos.
- Tuvo su **auge en internet** a partir de la invención de Napster, BitTorrent, etc.

### Flujos de Comunicación

![Esquema mixto: un Server central para descubrimiento, con Peers comunicándose entre sí](Clase09_imagenes/pag-09.png)

- Es **muy difícil** de establecer la comunicación entre pares directamente, por lo que se suele usar:
  - Un **esquema mixto** tipo cliente-servidor para proveer un **servicio de nombres** (descubrimiento de otros peers).
  - Un **grupo de comunicación** donde se comparte la dirección de los miembros.
- Requieren mayores permisos de *networking* (reglas de firewall de entrada, rangos de puertos, etc.), ya que cada peer debe ser alcanzable por los demás.

---

## 3. RPC (Remote Procedure Call)

![Cliente llamando a doOperation(input) y recibiendo doOperationResult del Server](Clase09_imagenes/pag-11.png)

- Permite la **ejecución remota de procedimientos**.
- Modelo Cliente-Servidor:
  - El cliente realiza una llamada a un procedimiento.
  - El servidor responde con el resultado de la operación.
- Comunicación remota **transparente** para el usuario.
- **Portabilidad** a través de la implementación de interfaces bien definidas.

### IDL (Interface Definition Language)

- Diseñados para permitir que diferentes lenguajes puedan invocarse entre sí.
- La interfaz se define en función de **datos de entrada (Input)** y **datos de salida (Output)**:
  - Acceso a métodos permitido.
  - Pasaje de variables **por valor**.
  - **Punteros no permitidos**.
- Define también los tipos de mensajes a enviar como parte del IDL.
- Ejemplo: Google Protocol Buffers.

### Tolerancia a Fallos

- A diferencia de las *Local Procedure Calls* (LPCs), un procedimiento remoto puede o no ser ejecutado (puede fallar la red).
- Diferentes estrategias para garantizar el *delivery* de mensajes:
  - *Request-Retry* con *Timeout*.
  - Filtrado de operaciones duplicadas.
  - Retransmisión / Re-ejecución de la operación si se pierde el *retry*.

### Call Semantics

Según las estrategias adoptadas para asegurar el *delivery* de mensajes, los mensajes pueden llegar a ser recibidos 0, 1 o muchas veces:

| Estrategia | Tipo de Control | Retry - Request | Filtro Duplicados | ¿Mensaje recibido? |
|---|---|---|---|---|
| #1 | Sin control | No | No implementable | *Maybe* |
| #2 | Re-ejecución | Sí | No | *At Least Once* |
| #3 | Retransmisión | Sí | Sí | *Exactly Once* |

### Implementación

- **Cliente**: conectado a un *stub*, realiza llamadas de forma transparente al servidor (o no tanto).
- **Servidor**: conectado a un *stub* del cual recibe parámetros; posee la lógica particular del *remote procedure*.
- **Stubs**: administran el *marshalling* de la información; envían información de llamadas (*calls*) al módulo de comunicación y al cliente/servidor.
- **Módulo de comunicación**: abstrae al *stub* de la comunicación con el servidor.

![Flujo detallado de una llamada RPC: Procedure, Marshal, Send/Receive, Transmit, Unmarshal, ReturnCall](Clase09_imagenes/pag-16.png)

El flujo completo de una llamada RPC: el cliente invoca el `Procedure()` → el stub realiza el `Marshal()` de los parámetros → se hace `Send()`/`Transmit()` hacia el servidor → el stub del servidor hace `Receive()`/`Unmarshal()` → se invoca el `Procedure()` real en el servidor → la respuesta vuelve siguiendo el camino inverso (`Marshal()` → `Send()` → `Transmit()` → `Receive()` → `Unmarshal()` → `ReturnCall()`).

### gRPC

![Cliente y Servidor comunicados mediante gRPC Stub y gRPC Server sobre proto/HTTP](Clase09_imagenes/pag-17.png)

- Definición de RPC basada en:
  - **HTTP2** para transporte.
  - **Protocol Buffers** para encoding.
  - Conexión punto a punto basada en `server:port`.
- Definición de Servicios y Mensajes en archivos `.proto`.
- Generación de código en distintos lenguajes.
- Diseñado para **alta performance** y **microservicios**.

---

## 4. Distributed Objects (Objetos Distribuidos)

![Server exponiendo Object 1 y Object 2 a través de un Dispatcher, consultado por varios Clients](Clase09_imagenes/pag-19.png)

- Los servidores ya no proveen **servicios** sino **objetos**.
- Existe un **middleware** que oculta la complejidad de:
  - Referencias a objetos remotos.
  - Invocación de acciones.
  - Errores (excepciones).
  - Recolección de basura (garbage collection).

### RPC vs Objetos Distribuidos

![Comparación Stateless (RPC Locator) vs Stateful (Dispatcher con migración/replicación de objetos)](Clase09_imagenes/pag-20.png)

- **RPC** (Stateless): el cliente local invoca procedimientos remotos a través de un *RPC Locator*, sin mantener estado de objetos particulares.
- **Objetos Distribuidos** (Stateful): el cliente invoca métodos sobre objetos específicos (ej. `obj1.m()`) que pueden ser **migrados o replicados** entre distintos servidores remotos, manteniendo su estado.

### CORBA

![Arquitectura CORBA: Client con Stub y Server con Skeleton sobre un Object Request Broker (ORB)](Clase09_imagenes/pag-21.png)

- Estándar definido por comité, con soporte en múltiples lenguajes.
- Actualmente **en vías de deprecación**.
- Provee:
  - Protocolo y serialización.
  - Transporte.
  - Seguridad.
  - *Discovery* de Objetos.
- Arquitectura: el Cliente usa un **Stub** y el Servidor un **Skeleton** + **Object Adaptor (POA)**, ambos comunicándose a través del **Object Request Broker (ORB)**.

**Flujo de trabajo con CORBA (ejemplo BankAccount):**
1. Se define la interfaz en un archivo **IDL** (`BankAccount.idl`) con los módulos/interfaces y sus operaciones (ej. `getValue()`, `add(in Money m)`).
2. Se generan automáticamente las clases de cliente (`BankAccountOperations.java`, `BankAccount.java`, `_BankAccountStub.java`, etc.) y de servidor (`BankAccountPOA.java`, etc.) mediante el compilador IDL (`idlj`).
3. El **cliente** (`ClientApp.java`) inicializa el ORB, resuelve el `NamingService`, busca la referencia remota (`BankAccount`) y la invoca de forma transparente como si fuera un objeto local.
4. El **servidor** (`BankAccountImpl.java`) extiende la clase POA generada, implementa la lógica real de las operaciones, se registra en el `NamingService` y queda a la espera de invocaciones de clientes.

### RMI (Remote Method Invocation)

![Cliente y Servidor conectados a través de Stub/Skeleton y Remote Reference Layer, con un Registry para el descubrimiento](Clase09_imagenes/pag-27.png)

- Versión optimizada de Distributed Objects, propia de **Java**.
- Requiere los siguientes pasos:
  1. **Registro** del servidor en un directorio de servicios (Registry).
  2. **Consulta** del registro por parte del cliente.
  3. **Invocación** desde el cliente al servidor.
- Arquitectura: Cliente con `Stub` y Servidor con `Skeleton`, ambos sobre la **Remote Reference Layer (RRL)**.

**Flujo de trabajo con RMI (ejemplo BankAccount):**
1. Se define la interfaz remota extendiendo `java.rmi.Remote`, declarando que cada método puede lanzar `RemoteException` (ej. `Money.java`, `BankAccount.java`).
2. La implementación del servidor (`MoneyImpl.java`, `BankAccountImpl.java`) extiende `UnicastRemoteObject` e implementa la interfaz remota con la lógica real.
3. El servidor (`ServerApp.java`) registra la instancia remota con `Naming.rebind("//localhost/BankAccount", new BankAccountImpl(...))`.
4. El cliente (`ClientApp.java`) obtiene una referencia remota con `Naming.lookup("//localhost/BankAccount")` y la invoca como si fuera un objeto local (ej. `a.getBalance()`, `a.add(m0)`).
