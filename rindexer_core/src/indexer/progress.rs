use crate::generator::event_callback_registry::EventInformation;
use crossterm::event::{KeyCode, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{
    event, execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ethers::middleware::Middleware;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::io::Stdout;
use std::sync::Arc;
use num_format::{Locale, ToFormattedString};
use tokio::sync::Mutex;
use tui::style::{Color, Style};
use tui::widgets::{Cell, Row, Table};
use tui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders},
    Frame, Terminal,
};

#[derive(Clone, Debug, Hash)]
pub enum IndexingEventProgressStatus {
    Syncing,
    Live,
    Completed,
    Failed,
}

impl IndexingEventProgressStatus {
    fn as_str(&self) -> &str {
        match self {
            Self::Syncing => "Syncing",
            Self::Live => "Live",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexingEventProgress {
    pub id: String,
    pub contract_name: String,
    pub event_name: String,
    pub last_synced_block: u64,
    pub syncing_to_block: u64,
    pub network: String,
    pub live_indexing: bool,
    pub status: IndexingEventProgressStatus,
    pub progress: f64,
}

impl Hash for IndexingEventProgress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.contract_name.hash(state);
        self.event_name.hash(state);
        self.last_synced_block.hash(state);
        self.syncing_to_block.hash(state);
        self.network.hash(state);
        self.live_indexing.hash(state);
        self.status.hash(state);
        let progress_int = (self.progress * 1_000.0) as u64;
        progress_int.hash(state);
    }
}

impl IndexingEventProgress {
    fn running(
        id: String,
        contract_name: String,
        event_name: String,
        last_synced_block: u64,
        syncing_to_block: u64,
        network: String,
        live_indexing: bool,
    ) -> Self {
        Self {
            id,
            contract_name,
            event_name,
            last_synced_block,
            syncing_to_block,
            network,
            live_indexing,
            status: IndexingEventProgressStatus::Syncing,
            progress: 0.0,
        }
    }
}

pub struct IndexingEventsProgressState {
    pub events: Vec<IndexingEventProgress>,
}

impl IndexingEventsProgressState {
    pub async fn monitor(
        event_information: Vec<EventInformation>,
    ) -> Arc<Mutex<IndexingEventsProgressState>> {
        let mut events = Vec::new();
        for event_information in event_information {
            for network_contract in event_information.contract.details {
                let latest_block = network_contract
                    .provider
                    .get_block_number()
                    .await
                    .unwrap()
                    .as_u64();
                events.push(IndexingEventProgress::running(
                    network_contract.id,
                    event_information.contract.name.to_string(),
                    event_information.event_name.to_string(),
                    network_contract.start_block.unwrap_or(0),
                    network_contract.end_block.unwrap_or(latest_block),
                    network_contract.network.clone(),
                    network_contract.end_block.is_none(),
                ));
            }
        }

        let state = Arc::new(Mutex::new(Self { events }));

        tokio::spawn(monitor_state_and_update_ui(state.clone()));

        state
    }

    pub fn update_last_synced_block(&mut self, id: &str, new_last_synced_block: u64) {
        for event in self.events.iter_mut() {
            if event.id == id {
                if event.progress != 1.0 {
                    if event.syncing_to_block > event.last_synced_block {
                        let total_blocks = event.syncing_to_block - event.last_synced_block;
                        let blocks_synced =
                            new_last_synced_block.saturating_sub(event.last_synced_block);

                        let effective_blocks_synced =
                            if new_last_synced_block > event.syncing_to_block {
                                total_blocks
                            } else {
                                blocks_synced
                            };

                        event.progress += (effective_blocks_synced as f64) / (total_blocks as f64);
                        event.progress = event.progress.clamp(0.0, 1.0);
                    }

                    if new_last_synced_block >= event.syncing_to_block {
                        event.progress = 1.0;
                        if event.live_indexing {
                            event.status = IndexingEventProgressStatus::Live;
                        } else {
                            event.status = IndexingEventProgressStatus::Completed;
                        }
                    }
                }

                event.last_synced_block = new_last_synced_block;

                break;
            }
        }
    }
}

pub fn setup_terminal() -> Result<(), io::Error> {
    let mut stdout = io::stdout();
    enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen)?;
    Ok(())
}

pub fn teardown_terminal() -> Result<(), io::Error> {
    let mut stdout = io::stdout();
    execute!(stdout, LeaveAlternateScreen)?;
    disable_raw_mode()?;
    Ok(())
}

async fn listen_for_exit_command() -> Result<(), std::io::Error> {
    loop {
        if event::poll(std::time::Duration::from_millis(500))? {
            if let event::Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') && key.modifiers == KeyModifiers::NONE {
                    return Ok(());
                }
            }
        }
    }
}

fn draw_ui(
    f: &mut Frame<CrosstermBackend<Stdout>>,
    events: &[IndexingEventProgress],
    scroll: usize,
) {
    let size = f.size();
    // Use the entire width for the table now that gauges are removed.
    let table_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0)]) // Use full height available
        .split(size);

    let visible_items = std::cmp::min(events.len(), size.height as usize / 3); // Adjusted for 3 lines per item.

    let table_rows: Vec<Row> = events
        .iter()
        .skip(scroll)
        .take(visible_items)
        .map(|event| {
            Row::new(vec![
                Cell::from(event.contract_name.clone()),
                Cell::from(event.event_name.clone()),
                Cell::from(event.last_synced_block.to_formatted_string(&Locale::en)),
                Cell::from(event.network.clone()),
                Cell::from(event.status.as_str().to_string()),
                Cell::from(format!("{:.2}%", event.progress * 100.0)),
            ])
        })
        .collect();

    let table = Table::new(table_rows)
        .header(
            Row::new(vec![
                "Contract",
                "Event",
                "Last Indexed",
                "Network",
                "Status",
                "Progress",
            ])
                .style(Style::default().fg(Color::Yellow)),
        )
        .block(
            Block::default()
                .title("Events Indexing Status")
                .borders(Borders::ALL),
        )
        .widths(&[
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(10),
            Constraint::Length(10),
        ]);

    f.render_widget(table, table_chunks[0]);
}


fn calculate_events_hash(events: &[IndexingEventProgress]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for event in events {
        event.hash(&mut hasher);
    }
    hasher.finish()
}

pub async fn monitor_state_and_update_ui(state: Arc<Mutex<IndexingEventsProgressState>>) {
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    setup_terminal().unwrap();

    tokio::spawn(async {
        listen_for_exit_command().await.unwrap();
        teardown_terminal().unwrap();
        println!("Exiting rindexer...");
        std::process::exit(0);
    });

    let mut scroll: usize = 0;

    if let Ok(true) = event::poll(std::time::Duration::from_millis(100)) {
        if let Ok(event::Event::Key(key)) = event::read() {
            match key.code {
                KeyCode::Down => scroll = scroll.saturating_add(1),
                KeyCode::Up => scroll = scroll.saturating_sub(1),
                _ => {}
            }
        }
    }

    let mut last_seen_hash = 0;

    loop {
        let state_lock = state.lock().await;
        let current_hash = calculate_events_hash(&state_lock.events);
        if last_seen_hash == current_hash {
            drop(state_lock);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }
        terminal
            .draw(|f| {
                draw_ui(f, &state_lock.events, scroll);
            })
            .unwrap();

        last_seen_hash = current_hash;
        drop(state_lock);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}
