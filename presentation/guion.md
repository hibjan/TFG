# Guion de Defensa — TFG

**Aplicación para la navegación en colecciones de contenidos multimedia**
Juan Andrés Hibjan Cardona · Leonardo Prado de Souza
10 minutos (≈5 cada uno)

## Reparto

| Sección                          | Speaker  | Tiempo         |
| -------------------------------- | -------- | -------------- |
| 1. Introducción                  | Juan     | ≤ 75s          |
| 2. Modelo de navegación          | Leonardo | ≤ 180s         |
| 3. Arquitectura y despliegue     | Juan     | ≤ 120s         |
| 4. Evaluación                    | Leonardo | ≤ 60s          |
| 5. Conclusiones + Trabajo futuro | Juan     | ≤ 60s          |
| 6. Contribuciones (EN)           | Ambos    | ≤ 2\*30s + 15s |

## 1. Introducción (75s) — Juan

### `hero`

Buenos días. Somos Juan Hibjan y Leonardo Prado. Presentamos nuestro Trabajo de Fin de Grado: una aplicación para navegar colecciones de contenidos multimedia.

### `overview-0`

### `intro`

Cuando exploramos catálogos grandes, las herramientas se quedan cortas. La búsqueda exige saber qué buscas. Los filtros clásicos son limitados. Y los recomendadores eliminan tu control. Hay tres mecanismos más potentes: filtrado facetado, navegación entre colecciones y operaciones de conjuntos. Pero hoy solo existen por separado.

### `objectives`

Nuestro objetivo: primero, definir un modelo formal que unifique los tres. Segundo, implementarlo como motor de exploración con API REST. Tercero, validarlo con usuarios sobre dos catálogos distintos.

### `process`

El desarrollo tuvo dos fases. Un prototipo CLI en Java con datos de TMDB, del que destilamos el modelo formal. Y después el sistema completo: API REST, cliente web e integración de DBLP para validar con usuarios.

⚡ **CAMBIO DE SPEAKER → Leonardo**

## 2. Modelo de navegación (180 s) — Leonardo

### `axes`

Vamos a ver el modelo con un ejemplo. Cuatro películas, sus personas y estudios. Tras cada acción el conjunto visible se reduce.

El Eje 1, Filtrado, acota dentro de una colección. El Eje 2, Navegación, salta entre colecciones por una relación tipada. El Eje 3, Unión, combina resultados parciales en una sola respuesta.

### `m-graph`

Este es el catálogo. Las entidades tienen metadatos —género, lugar de nacimiento— y se conectan por referencias tipadas: Actor, Director, Producción. La misma estructura sirve para películas o artículos científicos.

### `a1-0`

Eje 1: Filtrado. Queremos películas de acción, no dramáticas, de Marvel. Partimos de cuatro películas.

### `a1-1`

Incluimos el género Acción. Interstellar no lo tiene: desaparece. Quedan tres.

### `a1-2`

Excluimos Drama. Dark Knight Rises cae. Quedan dos.

### `a1-3`

Filtramos por relación: producida por Marvel. Inception es de Warner, cae. Queda The Avengers. Tres filtros combinados, una respuesta precisa.

### `a2-0`

Eje 2: Navegación. Nueva pregunta: ¿qué películas tienen un actor nacido en Los Ángeles? Esto no se resuelve con un filtro. Hay que cruzar colecciones.

### `a2-link`

Seguimos la relación Actor hacia Personas. Vemos tres actores. Nolan no aparece porque solo dirige.

### `a2-1`

Filtramos por lugar de nacimiento: Los Ángeles. Solo queda DiCaprio.

### `a2-2`

Ahora volvemos con goback. El contexto sobrevive. Vemos las películas de ese actor: Inception. Toda la ruta es una consulta compuesta.

### `a3-0`

Eje 3: Unión. Filtramos películas de ciencia ficción: Inception e Interstellar. Las guardamos.

### `a3-1`

Nuevo contexto en Personas: nacidos en Haverfordwest. Solo Bale. Lo añadimos.

### `a3-2`

La unión devuelve tres entidades de dos colecciones: Inception, Interstellar y Bale. Heterogénea por diseño.

### Dentro de una entidad (`shot-4-5-1` · `shot-4-5-3` · `shot-4-5-5`)

La exploración encuentra entidades; luego abres una. Aquí está The Dark Knight: su contenido multimedia — imágenes, texto y vínculos, y también soporta documentos y vídeo —, sus metadatos y sus referencias tipadas a otras entidades. Clicamos en una de esas referencias y saltamos directamente a la entidad conectada: Morgan Freeman, con su propio contenido y un vínculo externo a la fuente. La misma vista de detalle funciona para cualquier entidad de cualquier catálogo.

### `m-end`

Todo lo que hemos visto —filtrado, navegación y unión— se formaliza así: un estado es una tupla — la colección que estábamos viendo, los filtros que aplicamos y los enlaces que seguimos. Filtrado, link, goback y unión son operaciones con semántica precisa. El modelo es la contribución más reutilizable, independiente del lenguaje y del dominio.

⚡ **CAMBIO DE SPEAKER → Juan**

## 3. Arquitectura y despliegue (120 s) — Juan

### `arch-0`

Paso a explicar cómo llevamos el modelo a un sistema real. El frontend es Vanilla JS con Vite, sin frameworks. El backend, Servlets Java 17 sobre Tomcat. La base de datos, PostgreSQL con cinco tablas. Dos pipelines de datos: Python para TMDB y Go para DBLP. Y Docker Compose con Nginx y Cloudflare Tunnel en producción.

### `arch-1`

La arquitectura tiene cinco capas. El frontend envía HTTP/JSON a los Servlets. Debajo, el modelo: State, StateManager y Link, en la sesión HTTP. Y la capa de acceso a datos se conecta a PostgreSQL por JDBC. Las tres capas intermedias viven dentro de Tomcat.

### `arch-2`

Dos decisiones clave. Primera: la exploración es acumulativa, así que guardamos el estado en la sesión HTTP. Segunda: no materializamos resultados intermedios. El estado se traduce a una sola consulta SQL. Los filtros son cláusulas EXISTS. Los enlaces se convierten en subconsultas anidadas. La semántica formal se ejecuta directamente como SQL.

### `arch-3`

El despliegue: un solo comando. Docker Compose levanta cuatro contenedores: PostgreSQL, Tomcat, Nginx y Cloudflare. Nginx unifica frontend y API bajo un mismo origen. Cloudflare expone la app sin abrir puertos.

⚡ **CAMBIO DE SPEAKER → Leonardo**

## 4. Evaluación (60 s) — Leonardo

### `eval-methodology`

Para validar, dos líneas. Primero, validación funcional: el mismo motor maneja TMDB y DBLP sin cambiar código. Solo cambia la pipeline de datos. Segundo, un estudio de usabilidad con cinco participantes, siguiendo la regla de Nielsen que identifica el 85% de los problemas de usabilidad. Usamos protocolo think-aloud, catorce tareas por persona. Medimos con SUS y SEQ.

### `eval-results`

Resultados: SUS de 85, rango excelente. 93% de éxito: sesenta y cinco de setenta tareas completadas. Dos dominios validados. La principal fricción: el toggle incluir/excluir era difícil de descubrir. Es la prioridad para la siguiente iteración.

⚡ **CAMBIO DE SPEAKER → Juan**

## 5. Conclusiones y trabajo futuro (60 s) — Juan

### `conclusions`

En conclusión, entregamos tres cosas. Un motor que unifica filtrado, navegación y unión. Un modelo formal reutilizable, independiente del lenguaje y del dominio. Y una validación conceptual y empírica sobre dos catálogos.

### `future-work`

De cara al futuro, la extensión más prometedora: asistencia con LLMs, donde el modelo actúa como copiloto sin eliminar el control del usuario. Complementariamente, distribución entre nodos, persistencia con historial compartido, y extensión a nuevos dominios como bibliotecas y repositorios institucionales.

## 6. Contribuciones — en inglés (60 s) — Ambos

### `contributions`

_(Cambia automáticamente a inglés)_

**Juan (~30s):**

I was responsible for data and service infrastructure. I built the data pipelines: Python for TMDB and Go for DBLP. I implemented the persistence layer and the infrastructural servlets. And I handled deployment: Docker, Nginx, Cloudflare. In the thesis, I led the Architecture chapter.

**Leonardo (~30s):**

I built the full web client using modular Vanilla JS. I handled the visual and interaction design. On the backend, I implemented the faceted filters and set operations. I also designed and ran the usability study. In the thesis, I led the Testing and Validation chapter.

**Ambos (~15s):**

The navigation engine — State, StateManager and Link — was designed and built fifty-fifty, in shared sessions.

### `closing`

Muchas gracias. Quedamos a su disposición para preguntas.
