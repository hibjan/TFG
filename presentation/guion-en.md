# Defense Script (English) — Bachelor's Thesis

**Application for Browsing Multimedia Content Collections**
Juan Andrés Hibjan Cardona · Leonardo Prado de Souza
10 minutes (≈5 each)

> English version for the second defense. Slide anchors match `index.html`.
> Use the EN/ES toggle (top-right) and the ☀/🌙 light–dark toggle (top-left).

## Cast

| Section                          | Speaker  | Time           |
| -------------------------------- | -------- | -------------- |
| 1. Introduction                  | Juan     | ≤ 75s          |
| 2. Navigation model              | Leonardo | ≤ 180s         |
| 3. Architecture and deployment   | Juan     | ≤ 120s         |
| 4. Evaluation                    | Leonardo | ≤ 60s          |
| 5. Conclusions + Future work     | Juan     | ≤ 60s          |
| 6. Contributions                 | Both     | ≤ 2\*30s + 15s |

## 1. Introduction (75s) — Juan

### `hero`

Good morning. We are Juan Hibjan and Leonardo Prado. We present our Bachelor's thesis: an application for browsing multimedia content collections.

### `overview-0`

### `intro`

When we explore large catalogs, our tools fall short. Search demands that you already know what you're looking for. Classic filters are limited. And recommenders take away your control. There are three more powerful mechanisms: faceted filtering, navigation between collections, and set operations. But today they only exist in isolation.

### `objectives`

Our goal: first, to define a formal model that unifies the three. Second, to implement it as an exploration engine with a REST API. Third, to validate it with users on two structurally different catalogs.

### `process`

Development had two phases. A CLI prototype in Java with TMDB data, from which we distilled the formal model. And then the full system: a REST API, a web client, and DBLP integration to validate with users.

⚡ **SPEAKER CHANGE → Leonardo**

## 2. Navigation model (180 s) — Leonardo

### `axes`

Let's look at the model with an example. Four films, their people and studios. After each action, the visible set shrinks.

Axis 1, Filtering, narrows within a collection. Axis 2, Navigation, jumps across collections along a typed relation. Axis 3, Union, combines partial results into a single answer.

### `m-graph`

This is the catalog. Entities have metadata — genre, place of birth — and are connected by typed references: Actor, Director, Production. The same structure works for films or for scientific papers.

### `a1-0`

Axis 1: Filtering. We want action films, not dramatic, made by Marvel. We start from four films.

### `a1-1`

We include the genre Action. Interstellar doesn't have it: it disappears. Three remain.

### `a1-2`

We exclude Drama. The Dark Knight Rises drops out. Two remain.

### `a1-3`

We filter by relation: produced by Marvel. Inception is a Warner film, it drops. The Avengers remains. Three combined filters, one precise answer.

### `a2-0`

Axis 2: Navigation. A new question: which films feature an actor born in Los Angeles? A single filter can't answer this. We have to cross collections.

### `a2-link`

We follow the Actor relation toward People. We see three actors. Nolan doesn't appear, because he only directs.

### `a2-1`

We filter by place of birth: Los Angeles. Only DiCaprio remains.

### `a2-2`

Now we go back with goback. The context survives. We see that actor's films: Inception. The whole path is one composed query.

### `a3-0`

Axis 3: Union. We filter science-fiction films: Inception and Interstellar. We save them.

### `a3-1`

A new context on People: born in Haverfordwest. Only Bale. We add it.

### `a3-2`

The union returns three entities from two collections: Inception, Interstellar and Bale. Heterogeneous by design.

### Inside an entity (`shot-4-5-1` · `shot-4-5-3` · `shot-4-5-5`)

Exploration finds entities; you then open one. Here is The Dark Knight: its multimedia content — images, text and links, with support for documents and video too — its metadata, and its typed references to other entities. We click one of those references and jump straight to the connected entity: Morgan Freeman, with his own content and an external link back to the source. The same detail view works for any entity in any catalog.

### `m-end`

Everything we've just seen — filtering, navigation and union — is formalized like this: a state is a tuple — the collection we were viewing, the filters we applied, and the links we followed. Filtering, link, goback and union are operations with precise semantics. The model is the most reusable contribution, independent of the language and of the domain.

⚡ **SPEAKER CHANGE → Juan**

## 3. Architecture and deployment (120 s) — Juan

### `arch-0`

Now I'll explain how we took the model to a real system. The frontend is Vanilla JS with Vite, no frameworks. The backend, Java 17 Servlets on Tomcat. The database, PostgreSQL with five tables. Two data pipelines: Python for TMDB and Go for DBLP. And Docker Compose with Nginx and Cloudflare Tunnel in production.

### `arch-1`

The architecture has five layers. The frontend sends HTTP/JSON to the Servlets. Below that, the model: State, StateManager and Link, in the HTTP session. And the data-access layer connects to PostgreSQL over JDBC. The three middle layers live inside Tomcat.

### `arch-2`

Two key decisions. First: exploration is cumulative, so we keep the state in the HTTP session. Second: we don't materialize intermediate results. The state is translated into a single SQL query. Filters become EXISTS clauses. Links become nested sub-queries. The formal semantics runs directly as SQL.

### `arch-3`

Deployment: a single command. Docker Compose brings up four containers: PostgreSQL, Tomcat, Nginx and Cloudflare. Nginx unifies frontend and API under a single origin. Cloudflare exposes the app without opening any ports.

⚡ **SPEAKER CHANGE → Leonardo**

## 4. Evaluation (60 s) — Leonardo

### `eval-methodology`

To validate, two lines. First, functional validation: the same engine drives both TMDB and DBLP without changing code. Only the data pipeline changes. Second, a usability study with five participants, following Nielsen's rule that five users surface about 85% of usability problems. We used the think-aloud protocol, fourteen tasks per person. We measured with SUS and SEQ.

### `eval-results`

Results: a SUS of 85, the excellent range. 93% success: sixty-five of seventy tasks completed. Two domains validated. The main friction: the include/exclude toggle was hard to discover. It became the top interface fix for the next iteration.

⚡ **SPEAKER CHANGE → Juan**

## 5. Conclusions and future work (60 s) — Juan

### `conclusions`

In conclusion, we delivered three things. An engine that unifies filtering, navigation and union. A reusable formal model, independent of the language and of the domain. And a conceptual and empirical validation on two distinct catalogs.

### `future-work`

Looking ahead, the most promising extension: LLM assistance, where the model acts as a copilot without removing the user's control. In addition: distribution across nodes, persistence with shared history and reproducible exploration URLs, and extension to new domains such as libraries and institutional repositories.

## 6. Contributions (60 s) — Both

### `contributions`

_(The deck switches to English automatically.)_

**Juan (~30s):**

I was responsible for data and service infrastructure. I built the data pipelines: Python for TMDB and Go for DBLP. I implemented the persistence layer and the infrastructural servlets. And I handled deployment: Docker, Nginx, Cloudflare. In the thesis, I led the Architecture chapter.

**Leonardo (~30s):**

I built the full web client using modular Vanilla JS. I handled the visual and interaction design. On the backend, I implemented the faceted filters and set operations. I also designed and ran the usability study. In the thesis, I led the Testing and Validation chapter.

**Both (~15s):**

The navigation engine — State, StateManager and Link — was designed and built fifty-fifty, in shared sessions.

### `closing`

Thank you very much. We're happy to take your questions.
