//! Estado de OpenCode leyendo su API HTTP local (v2).
//!
//! OpenCode publica su endpoint y una contraseña efímera en `~/.local/state/opencode/service.json`
//! (`%USERPROFILE%\.local\state\opencode` en Windows). La isla se autentica con Basic
//! (`opencode:<contraseña>`) contra `127.0.0.1` y combina dos fuentes:
//!
//! - Un snapshot (`/api/session`, `/api/session/active`, `/api/permission/request`, `/api/model`)
//!   para estado, modelo, coste, tokens, contexto y permisos pendientes.
//! - El flujo SSE `/api/event`, que solo se usa como aviso de que algo cambió. Un debounce agrupa
//!   las ráfagas y vuelve a pedir el snapshot; así no dependemos de la forma exacta de cada delta.
//!
//! Si el servicio no está, se reintenta cada 5 s. Con el módulo apagado no se abre ninguna conexión.
//! La contraseña no se registra ni se envía fuera de localhost. Los permisos se observan, pero se
//! responden en OpenCode: la isla no escribe nada en su API.

use super::agent::{self, file_name, now_ms, truncate, AgentSession, AgentState, Status};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tauri::{AppHandle, Manager};

pub const KIND: &str = "opencode";
pub const OPENCODE: &str = "OpenCode";

/// Máximo de archivos tocados que se recuerdan por turno.
const MAX_FILES: usize = 8;
/// Una sesión sin actividad durante este tiempo no se muestra al arrancar ni se conserva.
const STALE_MS: i64 = 30 * 60_000;
/// Un turno "trabajando" sin eventos durante este tiempo se da por muerto.
const HUNG_MS: i64 = 10 * 60_000;
/// Al arrancar, una sesión terminada hace más de esto se muestra en espera, no como "listo".
const DONE_FRESH_MS: i64 = 2 * 60_000;
/// Espera entre reconexiones (mientras no hay SSE también hace de sondeo lento).
const RETRY: Duration = Duration::from_secs(5);
/// Tiempo máximo de espera de un evento SSE. OpenCode manda un latido cada ~15 s.
const SSE_TIMEOUT: Duration = Duration::from_secs(45);
/// Ráfagas de eventos más seguidas que esto se agrupan en un solo refresco.
const DEBOUNCE_MS: u128 = 400;
/// Sesiones que se piden al listar. OpenCode devuelve las más recientes primero.
const MAX_SESSION: usize = 100;

static RUNNING: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);

/// Registro local del servicio de OpenCode.
#[derive(Clone)]
pub struct Service {
    pub url: String,
    pub password: String,
}

pub fn set_enabled(app: &AppHandle, enabled: bool) {
    let mut running = RUNNING.lock().unwrap();
    match (enabled, running.is_some()) {
        (true, false) => {
            let flag = Arc::new(AtomicBool::new(true));
            let (app2, flag2) = (app.clone(), flag.clone());
            std::thread::Builder::new()
                .name("opencode-watch".into())
                .spawn(move || watch(app2, flag2))
                .ok();
            *running = Some(flag);
        }
        (false, true) => {
            if let Some(flag) = running.take() {
                flag.store(false, Ordering::Relaxed);
            }
            app.state::<AgentState>().sessions.lock().unwrap().retain(|_, s| s.kind != KIND);
            agent::emit(app);
        }
        _ => {}
    }
}

/// Carpeta de estado de OpenCode: `$XDG_STATE_HOME` o `~/.local/state`.
fn state_dir() -> Option<PathBuf> {
    if let Some(x) = std::env::var_os("XDG_STATE_HOME") {
        if !x.is_empty() {
            return Some(PathBuf::from(x));
        }
    }
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(|h| PathBuf::from(h).join(".local").join("state"))
}

/// Lee el registro que OpenCode reescribe al arrancar (el puerto es dinámico).
pub fn service() -> Option<Service> {
    let path = state_dir()?.join("opencode").join("service.json");
    parse_service(&std::fs::read_to_string(path).ok()?)
}

/// Valida y extrae `url` y `password`. Solo se acepta loopback: la contraseña no sale del equipo.
pub fn parse_service(text: &str) -> Option<Service> {
    let v: Value = serde_json::from_str(text).ok()?;
    let url = v.get("url")?.as_str()?.trim().trim_end_matches('/').to_string();
    let password = v.get("password")?.as_str()?.to_string();
    if url.is_empty() || password.is_empty() || !is_local(&url) {
        return None;
    }
    Some(Service { url, password })
}

fn is_local(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else { return false };
    if rest.starts_with("[::1]") {
        return true;
    }
    let host = rest.split([':', '/']).next().unwrap_or("");
    matches!(host, "127.0.0.1" | "localhost")
}

fn hostport(svc: &Service) -> &str {
    svc.url.trim_start_matches("http://").trim_end_matches('/')
}

/// GET mínimo por HTTP/1.0 (sin chunked) a la API local.
fn http_get(svc: &Service, path: &str) -> Option<Vec<u8>> {
    let host = hostport(svc);
    let addr = host.to_socket_addrs().ok()?.next()?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(600)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    let auth = BASE64.encode(format!("opencode:{}", svc.password));
    write!(s, "GET {path} HTTP/1.0\r\nHost: {host}\r\nAuthorization: Basic {auth}\r\nAccept: application/json\r\n\r\n").ok()?;
    let mut buf = Vec::new();
    s.take(8 * 1024 * 1024).read_to_end(&mut buf).ok()?;
    let i = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..i]);
    if !head.split_whitespace().nth(1).is_some_and(|c| c == "200") {
        return None;
    }
    Some(buf[i + 4..].to_vec())
}

fn get_json(svc: &Service, path: &str) -> Option<Value> {
    serde_json::from_slice(&http_get(svc, path)?).ok()
}

fn data_array(v: &Value) -> &[Value] {
    v.get("data").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

/// Abre el flujo SSE. Devuelve el lector y si el cuerpo va en chunks.
fn connect_sse(svc: &Service) -> Option<(BufReader<TcpStream>, bool)> {
    let host = hostport(svc);
    let addr = host.to_socket_addrs().ok()?.next()?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(600)).ok()?;
    s.set_read_timeout(Some(SSE_TIMEOUT)).ok()?;
    let auth = BASE64.encode(format!("opencode:{}", svc.password));
    write!(
        s,
        "GET /api/event HTTP/1.1\r\nHost: {host}\r\nAuthorization: Basic {auth}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\n\r\n"
    )
    .ok()?;
    let mut reader = BufReader::new(s);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        head.push_str(&line);
        if head.len() > 16 * 1024 {
            return None;
        }
    }
    if !head.starts_with("HTTP/1.1 200") {
        return None;
    }
    let chunked = head.to_ascii_lowercase().contains("transfer-encoding: chunked");
    Some((reader, chunked))
}

/// Lee un chunk del cuerpo. `None` al llegar al chunk cero.
fn read_chunk<R: BufRead>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let line = line.trim();
    if line.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "tamaño de chunk inválido"))?;
    if size == 0 {
        // Consume los trailers hasta la línea vacía.
        loop {
            let mut end = String::new();
            if r.read_line(&mut end)? == 0 || end.trim().is_empty() {
                break;
            }
        }
        return Ok(None);
    }
    let mut buf = vec![0u8; size];
    r.read_exact(&mut buf)?;
    let mut crlf = [0u8; 2];
    r.read_exact(&mut crlf)?;
    Ok(Some(buf))
}

/// Siguiente evento `data:` del flujo. `pending` conserva líneas partidas entre chunks.
pub fn read_sse_event<R: BufRead>(r: &mut R, chunked: bool, pending: &mut Vec<u8>) -> std::io::Result<Option<Value>> {
    loop {
        if let Some(pos) = pending.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = pending.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\r', '\n']);
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.trim();
                if !data.is_empty() {
                    if let Ok(v) = serde_json::from_str::<Value>(data) {
                        return Ok(Some(v));
                    }
                }
            }
            continue;
        }
        if chunked {
            match read_chunk(r)? {
                Some(bytes) => pending.extend_from_slice(&bytes),
                None => return Ok(None),
            }
        } else {
            let mut line = String::new();
            if r.read_line(&mut line)? == 0 {
                return Ok(None);
            }
            pending.extend_from_slice(line.as_bytes());
        }
    }
}

/// Qué hacer con un evento SSE.
pub enum Hint {
    /// Recargar la lista completa (sesión creada, borrada, archivada o reconexión).
    Full,
    /// Refrescar solo esta sesión.
    Session(String),
    /// Ignorar (deltas de texto, latidos...).
    Ignore,
}

pub fn event_hint(v: &Value) -> Hint {
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    if matches!(
        kind,
        "server.connected" | "session.created" | "session.deleted" | "session.archived" | "session.unarchived"
    ) {
        return Hint::Full;
    }
    // Los deltas llegan decenas de veces por segundo y no aportan nada al snapshot.
    if kind.ends_with(".delta") || kind.ends_with(".progress") || kind == "session.tool.input.started" {
        return Hint::Ignore;
    }
    match v.pointer("/data/sessionID").and_then(Value::as_str) {
        Some(id) => Hint::Session(id.to_string()),
        None => Hint::Ignore,
    }
}

/// Presencia de una sesión en el snapshot.
pub struct Presence {
    pub active: bool,
    pub permission: Option<String>,
    pub now: i64,
}

/// Aplica la información de `/api/session` (estado, proyecto, modelo, coste y tokens).
pub fn apply_session(s: &mut AgentSession, info: &Value, models: &[Value], p: &Presence) {
    if let Some(dir) = info.pointer("/location/directory").and_then(Value::as_str) {
        s.cwd = dir.to_string();
        let name = file_name(dir.trim_end_matches(['/', '\\']));
        if !name.is_empty() {
            s.project = name;
        }
    }
    if let Some(model) = info.get("model") {
        let (name, limit) = model_facts(models, model);
        if name.is_some() {
            s.usage.model = name;
        }
        if limit.is_some() {
            s.usage.context_size = limit;
        }
    }
    if let Some(cost) = info.get("cost").and_then(Value::as_f64) {
        s.usage.cost_usd = Some(cost);
    }
    if let Some(out) = info.pointer("/tokens/output").and_then(Value::as_u64) {
        s.usage.output_tokens = Some(out);
    }

    let updated = info.pointer("/time/updated").and_then(Value::as_i64).unwrap_or(p.now);
    let previous = s.status;

    if let Some(msg) = &p.permission {
        s.status = Status::Waiting;
        s.message = Some(msg.clone());
    } else if p.active {
        if previous == Status::Waiting {
            // Al responder el permiso, el mensaje vuelve al prompt del turno.
            s.message = None;
        } else if !matches!(previous, Status::Working) {
            s.turn_started_ms = Some(p.now);
            s.turn_ended_ms = None;
            s.files.clear();
            s.tool_count = 0;
            s.last_action = None;
            s.message = None;
        }
        s.status = Status::Working;
    } else {
        match info.get("outcome").and_then(Value::as_str) {
            Some("failed") => {
                s.status = Status::Error;
                s.turn_ended_ms = Some(updated);
            }
            Some("interrupted") => {
                s.status = Status::Done;
                s.turn_ended_ms = Some(updated);
                s.message = Some("Interrumpido".into());
            }
            Some("succeeded") => {
                if p.now - updated <= DONE_FRESH_MS || previous == Status::Working {
                    s.status = Status::Done;
                    s.turn_ended_ms = Some(updated);
                } else {
                    s.status = Status::Idle;
                }
            }
            _ => {
                if previous == Status::Working {
                    if p.now - s.last_event_ms <= HUNG_MS {
                        s.status = Status::Done;
                        s.turn_ended_ms = Some(updated);
                    } else {
                        s.status = Status::Idle;
                    }
                } else if previous == Status::Waiting {
                    // El permiso desapareció sin un final claro (p. ej. sesión cancelada).
                    s.status = Status::Idle;
                }
                if s.status == Status::Working && p.now - s.last_event_ms > HUNG_MS {
                    s.status = Status::Idle;
                }
            }
        }
    }

    s.last_event_ms = updated.max(if p.active { p.now } else { 0 });
}

/// Texto corto de un permiso pendiente.
pub fn permission_text(p: &Value) -> String {
    if let Some(m) = p.get("message").and_then(Value::as_str) {
        if !m.trim().is_empty() {
            return truncate(m, 120);
        }
    }
    let action = p.get("action").and_then(Value::as_str).unwrap_or("");
    let resource = p
        .get("resources")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .map(file_name)
        .unwrap_or_default();
    let head = if action.is_empty() { "Permiso pendiente".to_string() } else { format!("Permiso: {action}") };
    if resource.is_empty() {
        head
    } else {
        format!("{head} · {resource}")
    }
}

/// Nombre y límite de contexto de un modelo (`/api/model`).
pub fn model_facts(models: &[Value], model: &Value) -> (Option<String>, Option<u64>) {
    let pid = model.get("providerID").and_then(Value::as_str);
    let mid = model.get("id").and_then(Value::as_str);
    let hit = models.iter().find(|m| {
        m.get("providerID").and_then(Value::as_str) == pid
            && (m.get("modelID").and_then(Value::as_str) == mid || m.get("id").and_then(Value::as_str) == mid)
    });
    let name = hit
        .and_then(|m| m.get("name").and_then(Value::as_str))
        .map(String::from)
        .or_else(|| match (pid, mid) {
            (Some(p), Some(i)) => Some(format!("{p}/{i}")),
            _ => None,
        });
    let limit = hit.and_then(|m| m.pointer("/limit/context").and_then(Value::as_u64));
    (name, limit)
}

fn tool_path(input: Option<&Value>) -> Option<&str> {
    let i = input?;
    ["filePath", "file_path", "path", "file"].iter().find_map(|k| i.get(k).and_then(Value::as_str))
}

/// Resumen corto de una llamada a herramienta de OpenCode.
pub fn describe_tool(name: &str, input: Option<&Value>) -> String {
    let s = |k: &str| input.and_then(|v| v.get(k)).and_then(Value::as_str);
    let arg = |v: Option<&str>, n: usize| truncate(v.unwrap_or(""), n);
    match name {
        "shell" | "bash" => format!("Shell {}", arg(s("command"), 48)),
        "read" => format!("Read {}", tool_path(input).map(file_name).unwrap_or_default()),
        "edit" | "write" | "patch" => format!("Edit {}", tool_path(input).map(file_name).unwrap_or_default()),
        "grep" | "glob" => format!("Buscar {}", arg(s("pattern"), 40)),
        "webfetch" | "websearch" => format!("Web {}", arg(s("url").or_else(|| s("query")), 40)),
        "task" => format!("Task {}", arg(s("description"), 40)),
        "question" => "Pregunta".into(),
        "todowrite" => "Plan".into(),
        "" => "Herramienta".into(),
        other => other.to_string(),
    }
    .trim_end()
    .to_string()
}

fn is_edit(name: &str) -> bool {
    matches!(name, "edit" | "write" | "patch")
}

/// Aplica el último tramo de mensajes. Devuelve si encontró el prompt del usuario.
pub fn apply_messages(s: &mut AgentSession, msgs: &[Value]) -> bool {
    if msgs.is_empty() {
        return false;
    }
    let mut user: Option<String> = None;
    let mut newest: Option<&Value> = None;
    let mut usage: Option<&Value> = None;
    let mut tools = 0u32;
    let mut files: Vec<String> = Vec::new();
    let mut saw_assistant = false;

    // OpenCode devuelve los mensajes del más nuevo al más viejo: el primer "user" abre el turno.
    for m in msgs {
        match m.get("type").and_then(Value::as_str) {
            Some("user") => {
                user = m.get("text").and_then(Value::as_str).map(|t| truncate(t, 120));
                break;
            }
            Some("assistant") => {
                saw_assistant = true;
                if newest.is_none() {
                    newest = Some(m);
                }
                if usage.is_none() && m.get("tokens").is_some_and(|t| t.is_object()) {
                    usage = Some(m);
                }
                if let Some(content) = m.get("content").and_then(Value::as_array) {
                    tools += content.iter().filter(|c| c.get("type").and_then(Value::as_str) == Some("tool")).count() as u32;
                    for c in content.iter().rev() {
                        if c.get("type").and_then(Value::as_str) != Some("tool") {
                            continue;
                        }
                        let name = c.get("name").and_then(Value::as_str).unwrap_or("");
                        if !is_edit(name) {
                            continue;
                        }
                        if let Some(p) = tool_path(c.pointer("/state/input")) {
                            push_file(&mut files, p);
                        }
                    }
                }
                if let Some(snapshot) = m.pointer("/snapshot/files").and_then(Value::as_array) {
                    for f in snapshot.iter().rev() {
                        if let Some(p) = f.as_str() {
                            push_file(&mut files, p);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let assistant_text = newest
        .and_then(|a| a.get("content").and_then(Value::as_array))
        .and_then(|c| c.iter().rev().find(|x| x.get("type").and_then(Value::as_str) == Some("text")))
        .and_then(|x| x.get("text").and_then(Value::as_str));

    if let Some(a) = newest {
        if let Some(c) = a.get("content").and_then(Value::as_array) {
            if let Some(t) = c.iter().rev().find(|x| x.get("type").and_then(Value::as_str) == Some("tool")) {
                let name = t.get("name").and_then(Value::as_str).unwrap_or("");
                s.last_action = Some(describe_tool(name, t.pointer("/state/input")));
            }
        }
    }

    if saw_assistant {
        s.tool_count = tools;
        for f in files.into_iter().rev() {
            s.files.retain(|x| x != &f);
            s.files.insert(0, f);
        }
        s.files.truncate(MAX_FILES);
    }

    if s.status != Status::Waiting {
        if s.status == Status::Working {
            if let Some(u) = user.as_deref() {
                s.message = Some(u.to_string());
            }
        } else if let Some(t) = assistant_text {
            s.message = Some(truncate(t, 160));
        } else if let Some(u) = user.as_deref() {
            s.message = Some(u.to_string());
        }
    }
    if s.status == Status::Error {
        if let Some(err) = newest.and_then(|a| a.pointer("/error/message")).and_then(Value::as_str) {
            s.message = Some(truncate(err, 160));
        }
    }

    if let Some(m) = usage {
        if let Some(t) = m.get("tokens") {
            let n = |k: &str| t.get(k).and_then(Value::as_u64).unwrap_or(0);
            let ctx = n("input") + n("output") + t.pointer("/cache/read").and_then(Value::as_u64).unwrap_or(0);
            let ctx = ctx + t.pointer("/cache/write").and_then(Value::as_u64).unwrap_or(0);
            if ctx > 0 {
                s.usage.input_tokens = Some(ctx);
                if let Some(size) = s.usage.context_size.filter(|x| *x > 0) {
                    s.usage.context_pct = Some(ctx as f64 * 100.0 / size as f64);
                }
            }
            if s.usage.output_tokens.is_none() {
                s.usage.output_tokens = t.get("output").and_then(Value::as_u64);
            }
        }
    }

    user.is_some()
}

fn push_file(files: &mut Vec<String>, path: &str) {
    let f = file_name(path);
    if !f.is_empty() && !files.contains(&f) {
        files.push(f);
    }
}

fn fetch_active(svc: &Service) -> Option<HashSet<String>> {
    let v = get_json(svc, "/api/session/active")?;
    Some(v.get("data")?.as_object()?.keys().cloned().collect())
}

fn fetch_permissions(svc: &Service) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(v) = get_json(svc, "/api/permission/request") {
        for p in data_array(&v) {
            if let Some(sid) = p.get("sessionID").and_then(Value::as_str) {
                out.entry(sid.to_string()).or_insert_with(|| permission_text(p));
            }
        }
    }
    out
}

fn fetch_models(svc: &Service) -> Vec<Value> {
    get_json(svc, "/api/model")
        .and_then(|v| v.get("data").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

/// Rellena prompt, herramientas y archivos con los últimos mensajes del turno.
fn hydrate(svc: &Service, sid: &str, s: &mut AgentSession) {
    let mut saw_user = false;
    if let Some(v) = get_json(svc, &format!("/api/session/{sid}/message?limit=12")) {
        saw_user = apply_messages(s, data_array(&v));
    }
    // En turnos largos el prompt puede quedar fuera de la ventana de mensajes.
    if !saw_user && s.status == Status::Working && s.message.is_none() {
        if let Some(v) = get_json(svc, &format!("/api/session/{sid}/message?type=user&limit=1")) {
            if let Some(text) = data_array(&v).first().and_then(|m| m.get("text")).and_then(Value::as_str) {
                s.message = Some(truncate(text, 120));
            }
        }
    }
}

fn upsert(
    map: &mut HashMap<String, AgentSession>,
    info: &Value,
    models: &[Value],
    active: &HashSet<String>,
    perms: &HashMap<String, String>,
    now: i64,
) {
    let Some(sid) = info.get("id").and_then(Value::as_str) else { return };
    let id = format!("{KIND}:{sid}");
    let s = map
        .entry(id.clone())
        .or_insert_with(|| AgentSession { id, kind: KIND.into(), agent: OPENCODE.into(), ..Default::default() });
    let presence = Presence { active: active.contains(sid), permission: perms.get(sid).cloned(), now };
    apply_session(s, info, models, &presence);
}

/// Lectura puntual para diagnóstico y para la prueba en vivo. La isla usa `refresh`.
pub fn probe() -> Option<Vec<AgentSession>> {
    let svc = service()?;
    let models = fetch_models(&svc);
    let active = fetch_active(&svc)?;
    let perms = fetch_permissions(&svc);
    let list = get_json(&svc, &format!("/api/session?limit={MAX_SESSION}"))?;
    let now = now_ms();
    let mut map = HashMap::new();
    for info in data_array(&list) {
        let Some(sid) = info.get("id").and_then(Value::as_str) else { continue };
        upsert(&mut map, info, &models, &active, &perms, now);
        if let Some(s) = map.get_mut(&format!("{KIND}:{sid}")) {
            hydrate(&svc, sid, s);
        }
    }
    let mut out: Vec<AgentSession> = map.into_values().collect();
    out.sort_by_key(|s| std::cmp::Reverse(s.last_event_ms));
    Some(out)
}

/// Rehace el snapshot. Con `full` recarga la lista; si no, solo las sesiones marcadas.
fn refresh(app: &AppHandle, svc: &Service, models: &[Value], dirty: &mut HashSet<String>, full: bool, flag: &AtomicBool) {
    if !flag.load(Ordering::Relaxed) {
        return;
    }
    let Some(active) = fetch_active(svc) else { return };
    let perms = fetch_permissions(svc);
    let now = now_ms();

    let list = if full {
        match get_json(svc, &format!("/api/session?limit={MAX_SESSION}")) {
            Some(v) => v.get("data").and_then(Value::as_array).cloned().unwrap_or_default(),
            None => return,
        }
    } else {
        Vec::new()
    };

    let st = app.state::<AgentState>();
    let mut map = st.sessions.lock().unwrap();
    if !flag.load(Ordering::Relaxed) {
        return;
    }

    if full {
        let mut seen = HashSet::new();
        for info in &list {
            if let Some(sid) = info.get("id").and_then(Value::as_str) {
                seen.insert(sid.to_string());
                upsert(&mut map, info, models, &active, &perms, now);
                if active.contains(sid) {
                    dirty.insert(sid.to_string());
                }
            }
        }
        let prefix = format!("{KIND}:");
        map.retain(|id, s| {
            s.kind != KIND || !id.starts_with(&prefix) || seen.contains(&id[prefix.len()..]) || active.contains(&id[prefix.len()..])
        });
    }

    for sid in dirty.drain().take(8) {
        let Some(info) = get_json(svc, &format!("/api/session/{sid}")) else { continue };
        upsert(&mut map, &info, models, &active, &perms, now);
        if let Some(s) = map.get_mut(&format!("{KIND}:{sid}")) {
            hydrate(svc, &sid, s);
        }
    }

    map.retain(|_, s| s.kind != KIND || now - s.last_event_ms < STALE_MS);
    drop(map);
    agent::emit(app);
}

/// Si OpenCode desaparece, los turnos abiertos no se quedan "trabajando" para siempre.
fn decay(app: &AppHandle, flag: &AtomicBool) {
    if !flag.load(Ordering::Relaxed) {
        return;
    }
    let now = now_ms();
    let mut changed = false;
    let st = app.state::<AgentState>();
    let mut map = st.sessions.lock().unwrap();
    if !flag.load(Ordering::Relaxed) {
        return;
    }
    for s in map.values_mut().filter(|s| s.kind == KIND) {
        if s.status == Status::Working && now - s.last_event_ms > HUNG_MS {
            s.status = Status::Idle;
            changed = true;
        }
    }
    drop(map);
    if changed {
        agent::emit(app);
    }
}

fn watch(app: AppHandle, flag: Arc<AtomicBool>) {
    let mut warned = false;
    while flag.load(Ordering::Relaxed) {
        let Some(svc) = service() else {
            if !warned {
                eprintln!("[opencode] no hay servicio local; se reintentará cada 5 s");
                warned = true;
            }
            decay(&app, &flag);
            std::thread::sleep(RETRY);
            continue;
        };
        warned = false;

        let models = fetch_models(&svc);
        let mut all = HashSet::new();
        refresh(&app, &svc, &models, &mut all, true, &flag);
        if !flag.load(Ordering::Relaxed) {
            return;
        }

        if let Some((mut reader, chunked)) = connect_sse(&svc) {
            let mut pending = Vec::new();
            let mut dirty = HashSet::new();
            let mut last = Instant::now();
            loop {
                if !flag.load(Ordering::Relaxed) {
                    return;
                }
                match read_sse_event(&mut reader, chunked, &mut pending) {
                    Ok(Some(ev)) => match event_hint(&ev) {
                        Hint::Full => {
                            let mut all = HashSet::new();
                            refresh(&app, &svc, &models, &mut all, true, &flag);
                            dirty.clear();
                            last = Instant::now();
                        }
                        Hint::Session(id) => {
                            dirty.insert(id);
                            let terminal = ev.get("type").and_then(Value::as_str).is_some_and(|t| {
                                t.starts_with("session.execution.")
                                    || t == "session.idle"
                                    || t == "permission.asked"
                                    || t == "permission.replied"
                            });
                            if terminal || last.elapsed().as_millis() >= DEBOUNCE_MS {
                                refresh(&app, &svc, &models, &mut dirty, false, &flag);
                                last = Instant::now();
                            }
                        }
                        Hint::Ignore => {}
                    },
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        decay(&app, &flag);
        std::thread::sleep(RETRY);
    }
}
