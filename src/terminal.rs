use std::{
    io::{self, Stdout, Write, stdout},
    panic,
    sync::Once,
};

use crossterm::{
    cursor,
    event::{DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture},
    execute,
    style::ResetColor,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

pub type Tui = Terminal<CrosstermBackend<Stdout>>;

pub struct TerminalSession {
    terminal: Tui,
    restored: bool,
}

impl TerminalSession {
    /// Takes ownership of the terminal and enters the interactive screen.
    ///
    /// # Errors
    ///
    /// Returns an error when raw mode or terminal initialization fails.
    pub fn enter() -> io::Result<Self> {
        install_panic_hook();
        enable_raw_mode()?;

        let mut output = stdout();
        if let Err(error) = execute!(
            output,
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableMouseCapture,
            cursor::Hide
        ) {
            let _ = disable_raw_mode();
            return Err(error);
        }

        let backend = CrosstermBackend::new(output);
        let terminal = match Terminal::new(backend) {
            Ok(terminal) => terminal,
            Err(error) => {
                let _ = restore_terminal();
                return Err(error);
            }
        };
        Ok(Self {
            terminal,
            restored: false,
        })
    }

    pub fn terminal_mut(&mut self) -> &mut Tui {
        &mut self.terminal
    }

    /// Restores the terminal modes held by this session.
    ///
    /// # Errors
    ///
    /// Returns an error when the terminal cannot be restored.
    pub fn restore(&mut self) -> io::Result<()> {
        if self.restored {
            return Ok(());
        }
        let result = restore_terminal();
        if result.is_ok() {
            self.restored = true;
        }
        result
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

fn restore_terminal() -> io::Result<()> {
    let raw_result = disable_raw_mode();
    let mut output = stdout();
    let screen_result = execute!(
        output,
        DisableMouseCapture,
        DisableBracketedPaste,
        ResetColor,
        cursor::Show,
        LeaveAlternateScreen
    );
    let _ = output.flush();
    raw_result.and(screen_result)
}

fn install_panic_hook() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let _ = restore_terminal();
            previous(info);
        }));
    });
}
