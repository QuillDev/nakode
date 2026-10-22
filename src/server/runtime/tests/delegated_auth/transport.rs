use std::sync::{Arc, Mutex};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TokenVersion {
    Current,
    Replacement,
    Other,
}

#[derive(Clone)]
pub(super) struct Observation {
    pub refresh: bool,
    pub cloud_account: bool,
    pub token: TokenVersion,
}

pub(super) struct AuthTransport {
    pub origin: String,
    observations: Arc<Mutex<Vec<Observation>>>,
    task: tokio::task::JoinHandle<()>,
}

impl AuthTransport {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture listener");
        let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::clone(&observations);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("fixture connection");
                let request = read_request(&mut socket).await;
                let refresh = request.starts_with("POST /oauth/token ");
                let observation = Observation {
                    refresh,
                    cloud_account: request.contains("chatgpt-account-id: cloud-provider-account"),
                    token: if request.contains("authorization: Bearer synthetic-current") {
                        TokenVersion::Current
                    } else if request.contains("authorization: Bearer synthetic-replacement") {
                        TokenVersion::Replacement
                    } else {
                        TokenVersion::Other
                    },
                };
                let authenticated =
                    observation.cloud_account && observation.token != TokenVersion::Other;
                received.lock().expect("observations").push(observation);
                let (status, content_type, body) = if refresh || !authenticated {
                    (
                        "401 Unauthorized",
                        "application/json",
                        "{\"error\":\"synthetic-secret-must-not-leak\"}",
                    )
                } else if request.starts_with("GET /codex/models?") {
                    (
                        "200 OK",
                        "application/json",
                        r#"{"models":[{"slug":"gpt-native","is_default":true,"context_window":272000}]}"#,
                    )
                } else {
                    assert!(request.starts_with("POST /codex/responses "));
                    (
                        "200 OK",
                        "text/event-stream",
                        concat!(
                            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture-response\"}}\n",
                            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"DELEGATE-LIVE-QUARTZ-8264\"}\n",
                            "data: [DONE]\n"
                        ),
                    )
                };
                socket.write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                ).as_bytes()).await.expect("fixture response");
            }
        });
        Self {
            origin,
            observations,
            task,
        }
    }

    pub fn observations(&self) -> Vec<Observation> {
        self.observations.lock().expect("observations").clone()
    }
}

impl Drop for AuthTransport {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let size = socket.read(&mut buffer).await.expect("fixture request");
        assert_ne!(size, 0);
        request.extend_from_slice(&buffer[..size]);
        assert!(request.len() <= 1024 * 1024);
        let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&request[..header_end]).expect("headers");
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("content length"))
            })
            .unwrap_or(0);
        if request.len() >= header_end + 4 + length {
            return String::from_utf8(request).expect("request text");
        }
    }
}
