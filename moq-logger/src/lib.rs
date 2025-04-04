use chrono::{ self, Utc };
use lazy_static::lazy_static;
use paho_mqtt as mqtt;
use serde::Serialize;
use serde_json;
use std::collections::HashMap;
use std::sync::mpsc::{ self, Receiver, Sender, TryRecvError };
use std::sync::Mutex;
use std::thread;
use std::time::{ Duration, Instant };
use std::mem;

// TODO find clean fix for filling in IP
pub const BROKER_ADDRESS: &str = "";
pub const CLIENT_ID: &str = "relay_logger_buffered_batch"; // Updated client ID
pub const LOG_TOPIC: &str = "logging-stream";
const BUFFER_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const MQTT_THREAD_SLEEP_DURATION: Duration = Duration::from_millis(50);

#[derive(Serialize)] // Needed for serde_json::to_string
struct MqttBatchPayload {
	#[serde(rename = "bufferLength")] // Match the desired JSON key
	buffer_length: usize,
	#[serde(rename = "messageArray")] // Match the desired JSON key
	message_array: Vec<String>, // This will hold the individual JSON log message strings
}

pub struct MqttLogger {
	sender: Sender<String>,
}

impl MqttLogger {
	fn new() -> Self {
		let (sender, receiver) = mpsc::channel::<String>();

		thread::spawn(move || {
			println!("[MQTT Buffer Thread] Starting MQTT client...");

			let create_options = mqtt::CreateOptionsBuilder
				::new()
				.client_id(CLIENT_ID.to_string()) // Use updated constant if changed
				.server_uri(BROKER_ADDRESS)
				.persistence(None)
				.finalize();

			let client = match mqtt::Client::new(create_options) {
				Ok(client) => client,
				Err(err) => {
					eprintln!("[MQTT Buffer Thread] Error creating MQTT client: {}. Thread exiting.", err);
					return;
				}
			};

			let conn_options = mqtt::ConnectOptionsBuilder
				::new()
				.keep_alive_interval(Duration::from_secs(30))
				.clean_session(true)
				.finalize();

			println!("[MQTT Buffer Thread] Connecting to MQTT broker at {}...", BROKER_ADDRESS);
			if let Err(e) = client.connect(conn_options) {
				eprintln!("[MQTT Buffer Thread] Error connecting to the broker: {}. Thread exiting.", e);
				return;
			}
			println!("[MQTT Buffer Thread] Successfully connected to the broker.");

			let mut buffer: Vec<String> = Vec::new();
			let mut last_flush_time = Instant::now();

			println!("[MQTT Buffer Thread] Waiting for log messages...");

			loop {
				match receiver.try_recv() {
					Ok(log_message) => {
						buffer.push(log_message);
					}
					Err(TryRecvError::Empty) => {
						if last_flush_time.elapsed() >= BUFFER_FLUSH_INTERVAL && !buffer.is_empty() {
							// *** Call the UPDATED flush_buffer ***
							flush_buffer_as_batch(&client, &mut buffer, LOG_TOPIC);
							last_flush_time = Instant::now();
						} else {
							thread::sleep(MQTT_THREAD_SLEEP_DURATION);
						}
					}
					Err(TryRecvError::Disconnected) => {
						println!(
							"[MQTT Buffer Thread] Log sender disconnected. Flushing final batch and shutting down."
						);
						flush_buffer_as_batch(&client, &mut buffer, LOG_TOPIC);
						break;
					}
				}

				if last_flush_time.elapsed() >= BUFFER_FLUSH_INTERVAL && !buffer.is_empty() {
					flush_buffer_as_batch(&client, &mut buffer, LOG_TOPIC);
					last_flush_time = Instant::now();
				}
			}

			println!("[MQTT Buffer Thread] Disconnecting from MQTT broker...");
			if let Err(e) = client.disconnect(None) {
				eprintln!("[MQTT Buffer Thread] Error disconnecting: {}", e);
			}
			println!("[MQTT Buffer Thread] MQTT client finished.");
		});

		MqttLogger { sender }
	}

	// log function remains the same
	pub fn log(&self, message: &str) {
		if let Err(e) = self.sender.send(message.to_string()) {
			eprintln!("Error sending log message to MQTT buffer thread: {}", e);
		}
	}
}

fn flush_buffer_as_batch(client: &mqtt::Client, buffer: &mut Vec<String>, topic: &str) {
	if buffer.is_empty() {
		return;
	}

	// Take ownership of the messages, clearing the original buffer efficiently.
	let messages_to_send = mem::take(buffer);
	let message_count = messages_to_send.len();

	// Create the batch payload struct
	let batch_payload = MqttBatchPayload {
		buffer_length: message_count,
		message_array: messages_to_send, // Move the owned Vec into the struct
	};

	match serde_json::to_string(&batch_payload) {
		Ok(json_payload) => {
			let msg = mqtt::Message::new(
				topic,
				json_payload.as_bytes().to_vec(), // Send the JSON string as bytes
				mqtt::QoS::AtLeastOnce
			);

			println!(
				"[MQTT Buffer Thread] Flushing batch of {} messages as single JSON payload ({} bytes).",
				message_count,
				json_payload.len()
			);

			// Publish the single message
			if let Err(e) = client.publish(msg) {
				eprintln!("[MQTT Buffer Thread] Error publishing batch message: {}", e);
			}
		}
		Err(e) => {
			eprintln!("[MQTT Buffer Thread] Failed to serialize batch payload for {} messages: {}", message_count, e);
		}
	}
}

lazy_static! {
	pub static ref LOGGER: Mutex<MqttLogger> = Mutex::new(MqttLogger::new());
}

pub fn log_message(message: &str) {
	match LOGGER.lock() {
		Ok(logger) => logger.log(message),
		Err(poisoned) => {
			eprintln!("MQTT Logger mutex poisoned: {}", poisoned);
		}
	}
}

#[derive(Debug)]
pub struct LogMessage {
	pub stream: String,
	pub eventType: String,
	pub vantagePointID: String,
	pub payload: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct TimedLogMessage {
	pub stream: String,
	pub eventType: String,
	pub vantagePointID: String,
	pub timestamp: i64,
	pub payload: HashMap<String, String>,
}

fn check_moq_log_type(log_type: &str) -> String {
	match log_type {
		// Supported types: prefix with "moq:"
		"connect" => "moq:connect".to_string(),
		"connect-accepted" => "moq:connect-accepted".to_string(),
		// Any other type: prefix with "moq-custom:"
		_ => format!("moq-custom:{}", log_type),
	}
}

pub fn mqtt_log(message: LogMessage) {
	// Check if the log type is valid
	let log_type = check_moq_log_type(&message.eventType);
	let log_entry = TimedLogMessage {
		timestamp: Utc::now().timestamp_millis(),
		vantagePointID: message.vantagePointID,
		stream: message.stream,
		eventType: log_type,
		payload: message.payload,
	};

	match serde_json::to_string(&log_entry) {
		Ok(json_string) => {
			log_message(&json_string);
		}
		Err(e) => {
			eprintln!("Failed to serialize log message to JSON: {}", e);
		}
	}
}
