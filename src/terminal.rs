use crossterm::{
    cursor::{Hide, Show},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget, Wrap},
};
use std::collections::VecDeque;
use std::io::{self, IsTerminal};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const EVENT_BUFFER: usize = 1_024;
const BAR_WIDTH: usize = 24;
const TIMELINE_SIZE: usize = 4;
const FRAME_TIME: Duration = Duration::from_millis(50);

pub(crate) struct TerminalVisualizer {
    sender: mpsc::SyncSender<PoolEvent>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl TerminalVisualizer {
    pub(crate) fn new(workers: usize) -> Arc<Self> {
        let (sender, receiver) = mpsc::sync_channel(EVENT_BUFFER);
        let join = thread::spawn(move || render_loop(receiver, workers));

        Arc::new(Self {
            sender,
            join: Mutex::new(Some(join)),
        })
    }

    pub(crate) fn record(&self, event: PoolEvent) {
        // The UI is observational. A full UI channel must never slow a worker.
        let _ = self.sender.try_send(event);
    }

    pub(crate) fn finish(&self) {
        let _ = self.sender.send(PoolEvent::Closed);
        if let Some(join) = self.join.lock().unwrap().take() {
            let _ = join.join();
        }
    }
}

pub(crate) enum PoolEvent {
    Queued { task_id: u64 },
    Started { task_id: u64, worker_id: usize },
    Completed { task_id: u64, worker_id: usize },
    Panicked { task_id: u64, worker_id: usize },
    Closed,
}

struct RunningTask {
    task_id: u64,
    started: Instant,
}

struct TimelineEntry {
    task_id: u64,
    kind: TimelineKind,
    at: Instant,
}

enum TimelineKind {
    Queued,
    Started(usize),
    Completed(usize),
    Panicked(usize),
}

type Backend = CrosstermBackend<io::Stdout>;

/// Owns the terminal surface, so cleanup also happens if this thread unwinds.
struct TerminalSession {
    terminal: Terminal<Backend>,
}

impl TerminalSession {
    fn new() -> Option<Self> {
        if !io::stdout().is_terminal() {
            return None;
        }

        let mut stdout = io::stdout();
        if execute!(stdout, EnterAlternateScreen, Hide).is_err() {
            return None;
        }

        match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => Some(Self { terminal }),
            Err(_) => {
                let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn draw(
        &mut self,
        queued: &VecDeque<u64>,
        running: &[Option<RunningTask>],
        completed: usize,
        panicked: usize,
        timeline: &VecDeque<TimelineEntry>,
        started: Instant,
        frame: u64,
    ) -> io::Result<()> {
        self.terminal
            .draw(|terminal_frame| {
                render_dashboard(
                    terminal_frame.area(),
                    terminal_frame.buffer_mut(),
                    queued,
                    running,
                    completed,
                    panicked,
                    timeline,
                    started,
                    frame,
                );
            })
            .map(|_| ())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = self.terminal.show_cursor();
        let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
    }
}

fn render_loop(receiver: mpsc::Receiver<PoolEvent>, worker_count: usize) {
    let mut queued = VecDeque::new();
    let mut running = (0..worker_count).map(|_| None).collect::<Vec<_>>();
    let mut completed = 0;
    let mut panicked = 0;
    let mut timeline = VecDeque::with_capacity(TIMELINE_SIZE);
    let started = Instant::now();
    let mut frame = 0_u64;
    let mut terminal = TerminalSession::new();

    loop {
        let mut closed = false;
        match receiver.recv_timeout(FRAME_TIME) {
            Ok(PoolEvent::Closed) | Err(mpsc::RecvTimeoutError::Disconnected) => closed = true,
            Ok(event) => apply_event(
                event,
                &mut queued,
                &mut running,
                &mut completed,
                &mut panicked,
                &mut timeline,
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        while let Ok(event) = receiver.try_recv() {
            if matches!(event, PoolEvent::Closed) {
                closed = true;
            } else {
                apply_event(
                    event,
                    &mut queued,
                    &mut running,
                    &mut completed,
                    &mut panicked,
                    &mut timeline,
                );
            }
        }

        if terminal.as_mut().is_some_and(|session| {
            session
                .draw(
                    &queued, &running, completed, panicked, &timeline, started, frame,
                )
                .is_err()
        }) {
            // Losing a terminal is a display failure, not a pool failure.
            terminal = None;
        }
        frame = frame.wrapping_add(1);
        if closed {
            break;
        }
    }
}

fn apply_event(
    event: PoolEvent,
    queued: &mut VecDeque<u64>,
    running: &mut [Option<RunningTask>],
    completed: &mut usize,
    panicked: &mut usize,
    timeline: &mut VecDeque<TimelineEntry>,
) {
    let mut push_timeline = |task_id, kind| {
        if timeline.len() == TIMELINE_SIZE {
            timeline.pop_front();
        }
        timeline.push_back(TimelineEntry {
            task_id,
            kind,
            at: Instant::now(),
        });
    };

    match event {
        PoolEvent::Queued { task_id } => {
            queued.push_back(task_id);
            push_timeline(task_id, TimelineKind::Queued);
        }
        PoolEvent::Started { task_id, worker_id } => {
            if let Some(index) = queued.iter().position(|&queued_id| queued_id == task_id) {
                queued.remove(index);
            }
            running[worker_id] = Some(RunningTask {
                task_id,
                started: Instant::now(),
            });
            push_timeline(task_id, TimelineKind::Started(worker_id));
        }
        PoolEvent::Completed { task_id, worker_id } => {
            clear_running_task(&mut running[worker_id], task_id);
            *completed += 1;
            push_timeline(task_id, TimelineKind::Completed(worker_id));
        }
        PoolEvent::Panicked { task_id, worker_id } => {
            clear_running_task(&mut running[worker_id], task_id);
            *completed += 1;
            *panicked += 1;
            push_timeline(task_id, TimelineKind::Panicked(worker_id));
        }
        PoolEvent::Closed => {}
    }
}

fn clear_running_task(running: &mut Option<RunningTask>, task_id: u64) {
    if running.as_ref().is_some_and(|task| task.task_id == task_id) {
        *running = None;
    }
}

#[allow(clippy::too_many_arguments)]
fn render_dashboard(
    area: Rect,
    buffer: &mut ratatui::buffer::Buffer,
    queued: &VecDeque<u64>,
    running: &[Option<RunningTask>],
    completed: usize,
    panicked: usize,
    timeline: &VecDeque<TimelineEntry>,
    started: Instant,
    frame: u64,
) {
    let border_style = Style::default().fg(Color::Cyan);
    let frame_icon = if frame.is_multiple_of(2) { "✦" } else { "·" };
    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(Line::from(vec![
            Span::styled(
                format!(" {frame_icon} "),
                Style::default().fg(Color::Magenta),
            ),
            Span::styled(
                "POOLER",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  LIVE WORK-STEALING", Style::default().fg(Color::DarkGray)),
        ]));
    let inner = outer.inner(area);
    outer.render(area, buffer);

    let sections = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(4),
        Constraint::Length(1),
    ])
    .split(inner);

    render_header(
        sections[0],
        buffer,
        queued.len(),
        completed,
        panicked,
        started,
        frame_icon,
    );
    render_workers(sections[1], buffer, running, frame);
    render_flow(sections[2], buffer, queued, timeline, frame_icon);
    Paragraph::new(Line::from(vec![
        Span::styled("  ● ", Style::default().fg(Color::Green)),
        Span::styled(
            "workers report events; the renderer never blocks them",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .render(sections[3], buffer);
}

fn render_header(
    area: Rect,
    buffer: &mut ratatui::buffer::Buffer,
    queued: usize,
    completed: usize,
    panicked: usize,
    started: Instant,
    pulse: &str,
) {
    let mut spans = vec![Span::styled(
        format!("{pulse}  "),
        Style::default().fg(Color::Yellow),
    )];
    for (name, value, color) in [
        (
            "ELAPSED",
            format!("{:>5.2}s", started.elapsed().as_secs_f64()),
            Color::Yellow,
        ),
        ("QUEUED", queued.to_string(), Color::Magenta),
        ("DONE", completed.to_string(), Color::Green),
        ("PANIC", panicked.to_string(), Color::Red),
    ] {
        spans.push(Span::styled(
            format!(" {name} "),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            value,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw("   "));
    }
    let line = Line::from(spans);
    Paragraph::new(line)
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .render(area, buffer);
}

fn render_workers(
    area: Rect,
    buffer: &mut ratatui::buffer::Buffer,
    running: &[Option<RunningTask>],
    frame: u64,
) {
    let panel = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            " WORKERS ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
    let inner = panel.inner(area);
    panel.render(area, buffer);

    let rows = Layout::vertical(vec![Constraint::Length(2); running.len()]).split(inner);
    const SPINNER: [char; 8] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧'];
    for (worker_id, (worker_area, task)) in rows.iter().zip(running).enumerate() {
        let (mut headline, active) = match task {
            Some(task) => (
                vec![
                    Span::styled(
                        format!("  W{worker_id}  "),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("{} ", SPINNER[(frame as usize + worker_id) % SPINNER.len()]),
                        Style::default().fg(Color::Green),
                    ),
                    Span::styled(
                        "RUNNING  ",
                        Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!(" T{:03} ", task.task_id),
                        Style::default().fg(Color::Cyan).bg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!("  {:>5.2}s", task.started.elapsed().as_secs_f64()),
                        Style::default().fg(Color::Yellow),
                    ),
                ],
                true,
            ),
            None => (
                vec![
                    Span::styled(
                        format!("  W{worker_id}  "),
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled("· ", Style::default().fg(Color::DarkGray)),
                    Span::styled("IDLE", Style::default().fg(Color::Gray)),
                    Span::styled("  listening for work", Style::default().fg(Color::DarkGray)),
                ],
                false,
            ),
        };
        Paragraph::new(Line::from(std::mem::take(&mut headline))).render(
            Rect {
                height: 1,
                ..*worker_area
            },
            buffer,
        );

        let mut trace = vec![Span::styled("       ", Style::default())];
        trace.extend(activity_bar(frame as usize + worker_id * 6, active));
        Paragraph::new(Line::from(trace)).render(
            Rect {
                y: worker_area.y.saturating_add(1),
                height: 1,
                ..*worker_area
            },
            buffer,
        );
    }
}

fn render_flow(
    area: Rect,
    buffer: &mut ratatui::buffer::Buffer,
    queued: &VecDeque<u64>,
    timeline: &VecDeque<TimelineEntry>,
    pulse: &str,
) {
    let queue = queued
        .iter()
        .take(10)
        .map(|id| Span::styled(format!(" T{id:03} "), Style::default().fg(Color::Magenta)))
        .collect::<Vec<_>>();
    let mut queue_line = vec![Span::styled(
        " QUEUE ",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )];
    queue_line.extend(queue);
    if queued.len() > 10 {
        queue_line.push(Span::styled(
            format!(" +{} more", queued.len() - 10),
            Style::default().fg(Color::DarkGray),
        ));
    }

    let mut flow_line = vec![Span::styled(
        format!(" {pulse} FLOW  "),
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )];
    for (index, entry) in timeline.iter().enumerate() {
        if index != 0 {
            flow_line.push(Span::styled("  ›  ", Style::default().fg(Color::DarkGray)));
        }
        flow_line.extend(timeline_spans(entry));
    }
    Paragraph::new(vec![Line::from(queue_line), Line::from(flow_line)])
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .render(area, buffer);
}

fn activity_bar(phase: usize, active: bool) -> Vec<Span<'static>> {
    if !active {
        return vec![Span::styled(
            "░░░░░░░░░░░░░░░░░░░░░░░░",
            Style::default().fg(Color::DarkGray),
        )];
    }

    let mut bar = Vec::with_capacity(BAR_WIDTH);
    for index in 0..BAR_WIDTH {
        let distance = (index + BAR_WIDTH - phase % BAR_WIDTH) % BAR_WIDTH;
        let (symbol, style) = match distance {
            0 => (
                '█',
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            1..=2 => ('▓', Style::default().fg(Color::Cyan)),
            3..=5 => ('▒', Style::default().fg(Color::Blue)),
            _ => ('░', Style::default().fg(Color::DarkGray)),
        };
        bar.push(Span::styled(symbol.to_string(), style));
    }
    bar
}

fn timeline_spans(entry: &TimelineEntry) -> Vec<Span<'static>> {
    let emphasis = if entry.at.elapsed() < Duration::from_millis(350) {
        Modifier::BOLD
    } else {
        Modifier::empty()
    };
    match entry.kind {
        TimelineKind::Queued => vec![Span::styled(
            format!("T{:03} queued", entry.task_id),
            Style::default().fg(Color::Magenta).add_modifier(emphasis),
        )],
        TimelineKind::Started(worker_id) => vec![Span::styled(
            format!("W{worker_id} picked T{:03}", entry.task_id),
            Style::default().fg(Color::Blue).add_modifier(emphasis),
        )],
        TimelineKind::Completed(worker_id) => vec![Span::styled(
            format!("W{worker_id} ✓ T{:03}", entry.task_id),
            Style::default().fg(Color::Green).add_modifier(emphasis),
        )],
        TimelineKind::Panicked(worker_id) => vec![Span::styled(
            format!("W{worker_id} ! T{:03}", entry.task_id),
            Style::default().fg(Color::Red).add_modifier(emphasis),
        )],
    }
}
