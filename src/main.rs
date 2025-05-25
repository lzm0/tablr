use eframe::egui;
use eframe::egui::{ScrollArea, TextWrapMode};
use egui::widgets::Label;
use egui_extras::{Column, TableBody, TableBuilder, TableRow};
use polars::prelude::*;
use rfd::FileDialog;
use std::path::PathBuf;

struct Tablr {
    dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    files_to_load: Vec<PathBuf>,
    error_message: Option<String>,
    files_message: String,

    sort_column: Option<usize>,
    sort_descending: bool,
}

impl Tablr {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self {
            dataframe: None,
            column_names: Vec::new(),
            files_to_load: Vec::new(),
            error_message: None,
            files_message: String::new(),

            sort_column: None,
            sort_descending: false,
        }
    }

    fn load_parquet_data(&mut self, paths: Vec<PathBuf>) {
        self.dataframe = None;
        self.column_names.clear();

        let scan_sources = ScanSources::Paths(paths.into());

        match LazyFrame::scan_parquet_sources(scan_sources, ScanArgsParquet::default())
            .and_then(|lazy_frame| lazy_frame.collect())
        {
            Ok(df) => {
                self.column_names = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                self.dataframe = Some(df);
                self.error_message = None;
            }
            Err(e) => {
                self.dataframe = None;
                self.column_names.clear();
                self.error_message = Some(format!("Error processing Parquet files: {}", e));
            }
        }
    }

    fn process_pending_files(&mut self) {
        if !self.files_to_load.is_empty() {
            let files_to_load = std::mem::take(&mut self.files_to_load);
            self.load_parquet_data(files_to_load);
        }
    }

    fn render_file_selector(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if ui.button("Browse...").clicked() {
                self.handle_browse_button_click();
            }

            if self.files_message.is_empty() {
                ui.label("No parquet files selected.");
            } else {
                ui.label(format!("Selected: {}", self.files_message));
            }
        });
    }

    fn handle_browse_button_click(&mut self) {
        if let Some(paths) = FileDialog::new()
            .add_filter("Parquet files", &["parquet"])
            .set_title("Pick parquet file(s)")
            .pick_files()
        {
            if paths.is_empty() {
                self.files_message = "No files selected.".to_string();

                self.error_message =
                    Some("No files selected. Please select at least one Parquet file.".to_string());
            } else {
                self.files_message = if paths.len() == 1 {
                    paths[0]
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned()
                } else {
                    format!("{} files", paths.len())
                };
                self.files_to_load = paths;

                self.error_message = None;
            }
        }
    }

    fn render_error_message(&self, ui: &mut egui::Ui) {
        if let Some(err_msg) = &self.error_message {
            ui.colored_label(egui::Color32::RED, err_msg);
        }
    }

    fn render_dataframe(&mut self, ui: &mut egui::Ui) {
        if let Some(df) = &self.dataframe.clone() {
            ScrollArea::horizontal()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    TableBuilder::new(ui)
                        .striped(true)
                        .resizable(true)
                        .columns(Column::auto().resizable(true), self.column_names.len() + 1)
                        .header(20.0, |mut header_row| {
                            self.render_table_header(&mut header_row, &self.column_names.clone());
                        })
                        .body(|body| {
                            self.render_table_body(body, df, &self.column_names);
                        });
                });
        }
    }
    fn apply_sort(&mut self) {
        if let (Some(df), Some(col_idx)) = (&self.dataframe, self.sort_column) {
            let col_name = &self.column_names[col_idx];

            match df.sort(
                vec![PlSmallStr::from(col_name)],
                SortMultipleOptions::new().with_order_descending(self.sort_descending),
            ) {
                Ok(sorted_df) => {
                    self.dataframe = Some(sorted_df);
                }
                Err(e) => {
                    self.error_message = Some(format!("Sort error: {}", e));
                }
            }
        }
    }

    fn render_table_header(&mut self, header_row: &mut TableRow, column_names: &[String]) {
        header_row.col(|ui| {
            ui.add(cell_label("Row Index"));
        });

        for (i, col_name) in column_names.iter().enumerate() {
            header_row.col(|ui| {
                if ui
                    .add(cell_label(&format!(
                        "{}{}",
                        col_name,
                        if Some(i) == self.sort_column {
                            if self.sort_descending { "⬇" } else { "⬆" }
                        } else {
                            ""
                        }
                    )))
                    .clicked()
                {
                    if Some(i) == self.sort_column {
                        self.sort_descending = !self.sort_descending;
                    } else {
                        self.sort_column = Some(i);
                        self.sort_descending = false;
                    }
                    self.apply_sort();
                }
            });
        }
    }

    fn render_table_body(&self, body: TableBody, df: &DataFrame, column_names: &[String]) {
        let num_rows = df.height();
        body.rows(20.0, num_rows, |mut data_row_ui| {
            let row_index = data_row_ui.index();
            data_row_ui.col(|ui| {
                ui.add(cell_label(&row_index.to_string()));
            });

            for col_name in column_names {
                match df.column(col_name) {
                    Ok(column) => {
                        let cell_text = match column.get(row_index) {
                            Ok(any_value) => any_value.to_string(),
                            Err(_) => "Error".to_string(),
                        };
                        data_row_ui.col(|ui| {
                            ui.add(cell_label(&cell_text));
                        });
                    }
                    Err(_) => {
                        data_row_ui.col(|ui| {
                            ui.add(cell_label("Col?"));
                        });
                    }
                }
            }
        });
    }
}

fn cell_label(text: &str) -> Label {
    Label::new(text).wrap_mode(TextWrapMode::Extend)
}

impl eframe::App for Tablr {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_pending_files();

        egui::CentralPanel::default().show(ctx, |ui| {
            self.render_file_selector(ui);
            ui.separator();
            self.render_error_message(ui);
            self.render_dataframe(ui);
        });
    }
}

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Tablr - Parquet Viewer",
        options,
        Box::new(|cc| Ok(Box::new(Tablr::new(cc)))),
    )
}
