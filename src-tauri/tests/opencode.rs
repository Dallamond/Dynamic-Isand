use dynamic_island_lib::providers::agent::{AgentSession, Status};
use dynamic_island_lib::providers::opencode::*;
use serde_json::json;
use std::io::Cursor;

fn chunked(bytes: &[u8], size: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for part in bytes.chunks(size) {
        out.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
}

#[test]
fn registro_del_servicio() {
    let s = parse_service(r#"{"url":"http://127.0.0.1:28894/","password":"secreto","pid":1}"#).unwrap();
    assert_eq!(s.url, "http://127.0.0.1:28894");
    assert_eq!(s.password, "secreto");

    // Solo loopback: la contraseña no puede acabar en otra máquina.
    assert!(parse_service(r#"{"url":"http://192.168.1.20:28894","password":"x"}"#).is_none());
    assert!(parse_service(r#"{"url":"https://127.0.0.1:28894","password":"x"}"#).is_none());
    assert!(parse_service(r#"{"url":"http://127.0.0.1:28894"}"#).is_none());
    assert!(parse_service("basura").is_none());

    let v6 = parse_service(r#"{"url":"http://[::1]:1234","password":"x"}"#).unwrap();
    assert_eq!(v6.url, "http://[::1]:1234");
}

#[test]
fn sse_en_chunks_y_latidos() {
    let frames = b"data: {\"type\":\"session.idle\",\"data\":{\"sessionID\":\"s1\"}}\n\n: heartbeat\n\ndata: {\"type\":\"permission.asked\",\"data\":{\"sessionID\":\"s2\"}}\n\n";
    let mut r = Cursor::new(chunked(frames, 7));
    let mut pending = Vec::new();

    let a = read_sse_event(&mut r, true, &mut pending).unwrap().unwrap();
    assert_eq!(a["type"], "session.idle");
    let b = read_sse_event(&mut r, true, &mut pending).unwrap().unwrap();
    assert_eq!(b["type"], "permission.asked");
    assert_eq!(b["data"]["sessionID"], "s2");
    assert!(read_sse_event(&mut r, true, &mut pending).unwrap().is_none());
}

#[test]
fn eventos_que_refrescan() {
    assert!(matches!(event_hint(&json!({"type":"server.connected","data":{}})), Hint::Full));
    assert!(matches!(event_hint(&json!({"type":"session.created","data":{"sessionID":"s"}})), Hint::Full));
    assert!(matches!(event_hint(&json!({"type":"session.text.delta","data":{"sessionID":"s"}})), Hint::Ignore));
    assert!(matches!(event_hint(&json!({"type":"session.tool.called","data":{"sessionID":"s"}})), Hint::Session(id) if id == "s"));
    assert!(matches!(event_hint(&json!({"type":"permission.asked","data":{"sessionID":"s"}})), Hint::Session(id) if id == "s"));
}

#[test]
fn ciclo_de_una_sesion() {
    let now = 1_793_125_200_000;
    let models = vec![json!({"id":"m1","modelID":"m1","providerID":"p","name":"Modelo","limit":{"context":200_000}})];
    let mut info = json!({
        "id": "ses_1",
        "cost": 0.42,
        "tokens": {"input": 1000, "output": 250, "reasoning": 10, "cache": {"read": 0, "write": 0}},
        "model": {"id": "m1", "providerID": "p"},
        "time": {"created": now - 60_000, "updated": now - 1_000},
        "location": {"directory": r"D:\Proyectos\web"}
    });
    let mut s = AgentSession { id: "opencode:ses_1".into(), kind: KIND.into(), agent: OPENCODE.into(), ..Default::default() };

    apply_session(&mut s, &info, &models, &Presence { active: true, permission: None, now });
    assert_eq!(s.status, Status::Working);
    assert_eq!(s.project, "web");
    assert_eq!(s.usage.model.as_deref(), Some("Modelo"));
    assert_eq!(s.usage.context_size, Some(200_000));
    assert_eq!(s.usage.cost_usd, Some(0.42));
    assert_eq!(s.usage.output_tokens, Some(250));
    assert_eq!(s.turn_started_ms, Some(now));

    s.files.push("app.ts".into());
    apply_session(
        &mut s,
        &info,
        &models,
        &Presence { active: true, permission: Some("Permiso: edit · app.ts".into()), now },
    );
    assert_eq!(s.status, Status::Waiting);
    assert_eq!(s.message.as_deref(), Some("Permiso: edit · app.ts"));
    assert_eq!(s.files, vec!["app.ts"]);

    // Al responder, vuelve a trabajar sin perder archivos.
    apply_session(&mut s, &info, &models, &Presence { active: true, permission: None, now });
    assert_eq!(s.status, Status::Working);
    assert_eq!(s.message, None);
    assert_eq!(s.files, vec!["app.ts"]);

    info["outcome"] = json!("succeeded");
    info["time"]["updated"] = json!(now);
    apply_session(&mut s, &info, &models, &Presence { active: false, permission: None, now });
    assert_eq!(s.status, Status::Done);
    assert_eq!(s.turn_ended_ms, Some(now));
}

#[test]
fn sesion_vieja_no_aparece_como_lista() {
    let now = 1_793_125_200_000;
    let info = json!({
        "id": "ses_2",
        "outcome": "succeeded",
        "time": {"updated": now - 10 * 60_000},
        "location": {"directory": "C:/x/viejo"}
    });
    let mut s = AgentSession { id: "opencode:ses_2".into(), ..Default::default() };
    apply_session(&mut s, &info, &[], &Presence { active: false, permission: None, now });
    assert_eq!(s.status, Status::Idle);
    assert_eq!(s.project, "viejo");
}

#[test]
fn mensajes_del_turno() {
    let msgs = vec![
        json!({
            "type": "assistant",
            "content": [
                {"type": "text", "text": "Listo, he cambiado el header."},
                {"type": "tool", "name": "edit", "state": {"input": {"filePath": r"D:\web\Header.tsx"}}},
                {"type": "tool", "name": "shell", "state": {"input": {"command": "npm test"}}}
            ],
            "tokens": {"input": 3000, "output": 150, "reasoning": 20, "cache": {"read": 10000, "write": 0}},
            "snapshot": {"files": ["src/theme.css"]}
        }),
        json!({"type": "user", "text": "Añade modo oscuro"}),
    ];
    let mut s = AgentSession { status: Status::Working, ..Default::default() };
    s.usage.context_size = Some(20_000);

    assert!(apply_messages(&mut s, &msgs));
    assert_eq!(s.message.as_deref(), Some("Añade modo oscuro"));
    assert_eq!(s.last_action.as_deref(), Some("Shell npm test"));
    assert_eq!(s.tool_count, 2);
    assert_eq!(s.files, vec!["Header.tsx", "theme.css"]);
    assert_eq!(s.usage.input_tokens, Some(13_150));
    assert_eq!(s.usage.output_tokens, Some(150));
    assert!((s.usage.context_pct.unwrap() - 65.75).abs() < 0.01);

    // Al terminar, el mensaje pasa a ser la respuesta del agente.
    s.status = Status::Done;
    apply_messages(&mut s, &msgs);
    assert_eq!(s.message.as_deref(), Some("Listo, he cambiado el header."));
}

#[test]
fn resumen_de_herramientas() {
    assert_eq!(describe_tool("shell", Some(&json!({"command": "npm test"}))), "Shell npm test");
    assert_eq!(describe_tool("edit", Some(&json!({"filePath": "C:/x/app.ts"}))), "Edit app.ts");
    assert_eq!(describe_tool("read", Some(&json!({"path": "C:/x/lib.rs"}))), "Read lib.rs");
    assert_eq!(describe_tool("grep", Some(&json!({"pattern": "TODO"}))), "Buscar TODO");
    assert_eq!(describe_tool("question", None), "Pregunta");
    assert_eq!(describe_tool("desconocida", None), "desconocida");
    assert_eq!(describe_tool("edit", None), "Edit");
}

#[test]
fn permisos_y_modelos() {
    assert_eq!(
        permission_text(&json!({"action": "bash", "resources": ["C:/x/package.json"]})),
        "Permiso: bash · package.json"
    );
    assert_eq!(permission_text(&json!({"message": "¿Permitir red?"})), "¿Permitir red?");

    let models = vec![json!({"id":"m1","modelID":"m1","providerID":"p","name":"Modelo","limit":{"context":123_000}})];
    let (name, limit) = model_facts(&models, &json!({"id": "m1", "providerID": "p"}));
    assert_eq!(name.as_deref(), Some("Modelo"));
    assert_eq!(limit, Some(123_000));

    let (name, limit) = model_facts(&models, &json!({"id": "otro", "providerID": "p"}));
    assert_eq!(name.as_deref(), Some("p/otro"));
    assert_eq!(limit, None);
}

/// En vivo contra OpenCode: `cargo test --test opencode -- --ignored --nocapture`.
#[test]
#[ignore]
fn opencode_en_vivo() {
    let sessions = probe().expect("OpenCode no está corriendo o no publica service.json");
    for s in &sessions {
        println!(
            "{} · {} · {:?} · modelo {} · ctx {} · ${:?} · acción {:?} · mensaje {:?}",
            s.project,
            s.agent,
            s.status,
            s.usage.model.as_deref().unwrap_or("—"),
            s.usage.context_pct.map(|p| format!("{p:.0}%")).unwrap_or_else(|| "—".into()),
            s.usage.cost_usd,
            s.last_action,
            s.message
        );
    }
    assert!(!sessions.is_empty(), "OpenCode no tiene sesiones");
}

