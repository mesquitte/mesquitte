#[cfg(any(feature = "ws", feature = "wss"))]
use tungstenite::{handshake::server::ErrorResponse, http};

#[cfg(any(feature = "ws", feature = "wss"))]
pub fn ws_callback(
    req: &http::Request<()>,
    mut resp: http::Response<()>,
) -> Result<http::Response<()>, ErrorResponse> {
    if let Some(protocol) = req.headers().get("Sec-WebSocket-Protocol") {
        // see: [MQTT-6.0.0-3]
        if protocol != "mqtt" && protocol != "mqttv3.1" {
            log::info!("invalid WebSocket subprotocol name: {:?}", protocol);
            return Err(http::Response::new(Some(
                "invalid WebSocket subprotocol name".to_string(),
            )));
        }
        resp.headers_mut()
            .insert("Sec-WebSocket-Protocol", protocol.clone());
    }
    Ok(resp)
}
