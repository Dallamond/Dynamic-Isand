use dynamic_island_lib::providers::agent::{AgentSession, Status};
use dynamic_island_lib::providers::localai::*;
use serde_json::json;

#[test]
fn parse_lmstudio_elige_primer_modelo_cargado() {
    let data = json!({
        "data": [
            {"id": "nomic", "type": "embeddings", "state": "loaded"},
            {"id": "qwen/qwen3.5-9b", "type": "vlm", "state": "loaded", "loaded_context_length": 8192}
        ]
    });
    let loaded = parse_lmstudio(&data).unwrap();
    assert_eq!((loaded.model.as_str(), loaded.context), ("qwen/qwen3.5-9b", Some(8192)));
    assert_eq!(loaded.server, "LM Studio");
}

#[test]
fn parse_lmstudio_devuelve_none_si_no_hay_ninguno_cargado() {
    let data = json!({
        "data": [
            {"id": "nomic", "type": "embeddings", "state": "not-loaded"},
            {"id": "qwen/qwen3.5-9b", "type": "vlm", "state": "not-loaded"}
        ]
    });
    assert!(parse_lmstudio(&data).is_none());
}

#[test]
fn parse_lmstudio_devuelve_none_si_falta_data() {
    let data = json!({});
    assert!(parse_lmstudio(&data).is_none());
}

#[test]
fn parse_ollama_devuelve_primer_modelo_con_name() {
    let data = json!({
        "models": [
            {"name": "llama3.1:8b"},
            {"name": "llama2.5:4b"}
        ]
    });
    let loaded = parse_ollama(&data).unwrap();
    assert_eq!(loaded.model, "llama3.1:8b");
    assert_eq!(loaded.server, "Ollama");
}

#[test]
fn parse_ollama_devuelve_primer_modelo_con_model_si_no_hay_name() {
    let data = json!({
        "models": [
            {"model": "llama3.1:8b"},
            {"model": "llama2.5:4b"}
        ]
    });
    let loaded = parse_ollama(&data).unwrap();
    assert_eq!(loaded.model, "llama3.1:8b");
    assert_eq!(loaded.server, "Ollama");
}

#[test]
fn parse_ollama_con_vacia_devuelve_none() {
    let data = json!({"models": []});
    assert!(parse_ollama(&data).is_none());
}

#[test]
fn una_lectura_con_gpu_alta_no_pasa_a_generando() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, Some(60), 1_000);
    assert_ne!(s.status, Status::Working);
}

#[test]
fn dos_lecturas_con_gpu_alta_pasan_a_generando() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, Some(60), 1_000);
    step(&mut s, &mut t, &l, Some(60), 3_000);
    assert_eq!(s.status, Status::Working);
    assert_eq!(s.turn_started_ms, Some(3_000));
    assert_eq!(s.last_action.as_deref(), Some("Generando"));
}

#[test]
fn tras_generar_una_lectura_con_gpu_baja_termina_el_turno() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, Some(60), 1_000);
    step(&mut s, &mut t, &l, Some(60), 3_000);
    step(&mut s, &mut t, &l, Some(10), 5_000);
    assert_eq!(s.status, Status::Done);
    assert_eq!(s.turn_ended_ms, Some(5_000));
    assert_eq!(s.message.as_deref(), Some("Respuesta terminada"));
}

#[test]
fn un_pico_suelto_nunca_llega_a_generando() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, Some(60), 1_000);
    step(&mut s, &mut t, &l, Some(10), 3_000);
    step(&mut s, &mut t, &l, Some(60), 5_000);
    assert_ne!(s.status, Status::Working);
}

#[test]
fn sin_lectura_de_gpu_no_genera_ni_rellena_el_uso() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, None, 1_000);
    assert_ne!(s.status, Status::Working);
    assert_eq!(s.usage.gpu_pct, None);
}

#[test]
fn la_primera_lectura_rellena_modelo_proyecto_y_contexto() {
    let mut s = AgentSession::default();
    let mut t = Tracker::default();
    let l = Loaded { server: "LM Studio", model: "m".into(), context: Some(4096) };
    step(&mut s, &mut t, &l, Some(60), 1_000);
    assert_eq!(s.usage.model.as_deref(), Some("m"));
    assert_eq!(s.project, "m");
    assert_eq!(s.usage.context_size, Some(4096));
}
