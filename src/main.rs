use eframe::egui;
use eframe::egui::{ScrollArea, TextWrapMode};
use egui::widgets::Label;
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use rfd::FileDialog;
use std::path::PathBuf;

struct Tablr {
    dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    error_message: Option<String>,
    file_to_load: Option<PathBuf>,
    file_display_name: String,
}

impl Tablr {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self {
            dataframe: None,
            column_names: Vec::new(),
            error_message: None,
            file_to_load: None,
            file_display_name: String::new(),
        }
    }

    fn load_parquet_data(&mut self, file_path: &PathBuf) {
        self.dataframe = None;
        self.column_names.clear();
        self.error_message = None;

        let file = match std::fs::File::open(file_path) {
            Ok(f) => f,
            Err(e) => {
                self.error_message = Some(format!("Error opening file {:?}: {}", file_path, e));
                return;
            }
        };

        match ParquetReader::new(file).finish() {
            Ok(df) => {
                self.column_names = df
                    .get_column_names()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();

                self.dataframe = Some(df);
            }
            Err(e) => {
                self.error_message = Some(format!("Error reading Parquet file: {}", e));
            }
        }
    }
}

impl eframe::App for Tablr {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Some(path_to_load) = self.file_to_load.take() {
            self.load_parquet_data(&path_to_load);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Browse...").clicked() {
                    if let Some(path) = FileDialog::new()
                        .add_filter("Parquet files", &["parquet"])
                        .set_title("Pick a Parquet file")
                        .pick_file()
                    {
                        let file_path = path.display().to_string().trim().to_string();
                        self.file_display_name = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string();
                        if !file_path.is_empty() {
                            self.file_to_load = Some(PathBuf::from(file_path));
                        } else {
                            self.dataframe = None;
                            self.column_names.clear();
                            self.error_message = Some("Please select a file path.".to_string());
                        }
                    }
                }

                if self.file_display_name.is_empty() {
                    ui.label("No parquet file selected.");
                } else {
                    ui.label(format!("Loaded file: {}", self.file_display_name));
                }
            });

            ui.separator();

            if let Some(err_msg) = &self.error_message {
                ui.colored_label(egui::Color32::RED, err_msg);
            }

            if let Some(df) = &self.dataframe {
                if !self.column_names.is_empty() {
                    ScrollArea::horizontal()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            TableBuilder::new(ui)
                                .striped(true)
                                .resizable(true)
                                .columns(Column::auto().resizable(true), self.column_names.len())
                                .header(20.0, |mut header| {
                                    header.col(|ui| {
                                        ui.label("Row Index");
                                    });
                                    for col_name in &self.column_names {
                                        header.col(|ui| {
                                            ui.add(
                                                Label::new(col_name)
                                                    .wrap_mode(TextWrapMode::Extend),
                                            );
                                        });
                                    }
                                })
                                .body(|body| {
                                    let num_rows = df.height();
                                    body.rows(20.0, num_rows, |mut row| {
                                        let row_index = row.index();

                                        row.col(|ui| {
                                            ui.label(row_index.to_string());
                                        });

                                        for col_name in &self.column_names {
                                            match df.column(col_name) {
                                                Ok(series) => {
                                                    let cell_text = match series.get(row_index) {
                                                        Ok(any_value) => any_value.to_string(),
                                                        Err(_) => "Error (cell access)".to_string(),
                                                    };
                                                    row.col(|ui| {
                                                        ui.add(
                                                            Label::new(cell_text)
                                                                .wrap_mode(TextWrapMode::Extend),
                                                        );
                                                    });
                                                }
                                                Err(_) => {
                                                    row.col(|ui| {
                                                        ui.add(
                                                            Label::new("Error (column access)")
                                                                .wrap_mode(TextWrapMode::Extend),
                                                        );
                                                    });
                                                }
                                            }
                                        }
                                    });
                                });
                        });
                } else if self.error_message.is_none() {
                    ui.label("File loaded, but it contains no columns or data.");
                }
            }
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
